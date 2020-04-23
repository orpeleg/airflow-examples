import logging
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.exceptions import AirflowException


# implementation of emr add spark step operator using retry mechanism that samples the step state
# on attempt % 3 = 0 action on failure step is termination. e.g: on daily run if step failed 3 times,
# on 3rd attempt cluster will terminate, and cascades failure to the next job
# describe_step - is sampling emr step every 5 minutes. if step failed the operator will throw AirflowException
class EmrAddScriptStepOperator(EmrAddStepsOperator):

    template_fields = ['job_flow_id', 'script_path', 'script_params', 'action_on_failure', 'task_id', 'step_name',
                       'region']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            script_path=None,
            script_params='',
            region=None,
            action_on_failure='TERMINATE_JOB_FLOW',
            step_name='step',
            *args, **kwargs):

        if script_path is None:
            raise AirflowException('missing script_path')
        if region is None:
            raise AirflowException('missing region')

        super(EmrAddScriptStepOperator, self).__init__(*args, **kwargs)
        self.script_path = script_path
        self.script_params = script_params
        logging.info("script_params: " + str(script_params))
        self.region = region
        self.step_name = step_name
        self.action_on_failure = action_on_failure
        self.aws_conn_id = aws_conn_id
        self.steps = self.generate_script_step(step_name, script_path, script_params, action_on_failure, region)
        logging.info("script steps: " + str(self.steps))

    def execute(self, context):
        return super(EmrAddScriptStepOperator, self).execute(context=context)

    @staticmethod
    def generate_script_step(step_name, script_path, script_params, action_on_failure, region):
        command = str(script_path) + ' ' + str(script_params)
        runner = 's3://{}.elasticmapreduce/libs/script-runner/script-runner.jar'.format(region)
        step = [
            {
                'Name': step_name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': runner,
                    "Args": command.split()
                }
            }
        ]
        print(str(step))
        return step

