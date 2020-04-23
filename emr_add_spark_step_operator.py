import logging
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook
from retrying import retry


# implementation of emr add spark step operator using retry mechanism that samples the step state
# on attempt % 3 = 0 action on failure step is termination
class EmrAddSparkStepOperator(EmrAddStepsOperator):

    template_fields = ['job_flow_id', 'steps', 'main_class', 'app_name', 'spark_conf', 'spark_params',
                       'application_args', 'jar_path', 'action_on_failure', 'task_id', 'step_name', 'region']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            main_class=None,
            app_name=None,
            spark_params=None,
            spark_conf=None,
            application_args=None,
            jar_path=None,
            action_on_failure='TERMINATE_JOB_FLOW',
            step_name='step',
            region='eu-west-1',
            provide_context=True,
            *args, **kwargs):

        if app_name is None:
            raise AirflowException('missing app_name')
        if main_class is None:
            raise AirflowException('missing main_class')
        if jar_path is None:
            raise AirflowException('missing jar_path')

        super(EmrAddSparkStepOperator, self).__init__(*args, **kwargs)
        self.app_name = app_name
        self.main_class = main_class
        self.spark_conf = spark_conf
        self.spark_params = spark_params
        self.application_args = application_args
        self.jar_path = jar_path
        self.step_name = step_name
        self.action_on_failure = action_on_failure
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.provide_context = provide_context

    def execute(self, context):
        attempt = context['ti'].try_number
        logging.info('attempt: {}'.format(attempt))
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        job_flow_id = self.job_flow_id

        if not job_flow_id:
            job_flow_id = emr.get_cluster_id_by_name(self.job_flow_name, self.cluster_states)

        if self.do_xcom_push:
            context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)

        step_name = self.step_name if attempt == 1 else "{} (attempt {})".format(self.step_name, attempt)

        action_on_failure = self.action_on_failure
        if attempt % 3 == 0:
            action_on_failure = 'TERMINATE_JOB_FLOW'

        spark_conf = self.get_spark_params_config(self.spark_params, self.spark_conf)

        steps = self.generate_spark_step(step_name, self.main_class, self.app_name, spark_conf,
                                         self.application_args, self.jar_path, action_on_failure)
        logging.info("spark_params: " + str(steps))

        self.log.info('Adding steps to %s', job_flow_id)
        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)

        logging.info('Running Spark Job {} with JobFlow ID {}'.format(self.task_id, self.job_flow_id))
        while True:
            step_id = response['StepIds'][0]
            logging.info('step id - {}'.format(step_id))
            result = self.describe_step(emr, response)
            step_status = result['Step']['Status']['State']
            logging.info('step status - {}'.format(step_status))
            # step state can be 'PENDING'|'CANCEL_PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED'
            if step_status == 'COMPLETED':
                break
            elif step_status != 'COMPLETED' and step_status != 'PENDING' and step_status != 'RUNNING':
                raise AirflowException('Spark job {} has failed'.format(self.task_id))

            logging.info("Spark Job '{}' status is {}".format(self.task_id, step_status))

    @staticmethod
    def generate_spark_step(step_name, main_class, app_name, spark_conf, app_params, jar_path,
                            action_on_failure):
        command = ['spark-submit',
                   '--deploy-mode', 'cluster',
                   '--class', main_class,
                   '--master', 'yarn',
                   '--name', app_name
                   ]

        command += spark_conf
        command.append(str(jar_path))
        command += app_params

        logging.info('spark-submit command: {}'.format(command))
        runner = 'command-runner.jar'
        step = [
            {
                'Name': step_name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': runner,
                    "Args": command
                }
            }
        ]
        logging.info('step - {}'.format(str(step)))
        return step

    @staticmethod
    def get_spark_params_config(params, config):
        spark_conf = ''.join(['--conf %s=%s ' % (k, v) for k, v in config.items()])
        spark_params = ''.join(['--%s %s ' % (k, v) for k, v in params.items()])
        result = spark_params + spark_conf
        logging.info('spark configuration: {}'.format(result))
        return result.rstrip().split()

    # wait 2^i seconds between each retry up to 5m, stop after 30m
    @retry(wait_exponential_multiplier=1000,
           wait_exponential_max=300000,
           stop_max_delay=1800000)
    def describe_step(self, emr, response):
        return emr.describe_step(ClusterId=self.job_flow_id, StepId=response['StepIds'][0])
