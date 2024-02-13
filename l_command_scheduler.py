from mcdreforged.api.all import *
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import json


PLUGIN_METADATA = {
    "id": "l_command_scheduler",
    "version": "0.0.3",
    "name": "L-CommandScheduler",
    "link": "https://github.com/lllrmm/L-CommandScheduler",
    "description": {
        "en_us": "A MCDR plugin that schedules commands to run based on crontab.",
        "zh_cn": "一个基于crontab自动执行指令（组）的MCDR插件。"
    },
    "author": [
        "lllrmm"
    ],
}



SchedulerMng = None


def on_load(Server: PluginServerInterface, prev_module):
    global SchedulerMng
    Server.logger.info('L Scheduler loading now...')
    SchedulerMng = Lscheduler(Server)
    SchedulerMng.SchedulerStart()


def on_unload(Server: PluginServerInterface):
    global SchedulerMng
    SchedulerMng.SchedulerStop()
    SchedulerMng = None


''' # by ChatGPT
def register_help_message(Server: PluginServerInterface):
    # Add your help messages here
    help_messages = [
        ('!!lsche schedule <task_name>', 'Schedule a task with a given name.'),
        ('!!lsche cancel <task_name>', 'Cancel a scheduled task.'),
    ]

    for command, description in help_messages:
        Server.register_help_message(command, description)
'''



class Lscheduler:
    CONFIG_FILE_NAME = 'scheduler_plugin.json'
    TAG_ENABLED = 'enabled'
    TAG_TASKS = 'tasks'
    TAG_TASK_NAME = 'name'
    TAG_TASK_COMMANDS = 'commands'
    TAG_TASK_CRONTAB = 'crontab'
    TAG_COMMAND_TYPE = 'type'
    TAG_COMMAND_LINE = 'command'
    TAG_COMMAND_TYPE_MCDR = 'MCDR'
    TAG_COMMAND_TYPE_MC_SERVER = 'MCserver'


    def __init__(self, Server: PluginServerInterface):
        self.Server = Server
        self.Scheduler = BackgroundScheduler()
        self.Scheduler.add_executor('threadpool')
        self.schedulerStarted :bool = False


    def ConfigLoad(self):
        try:
            configPath = self.Server.get_data_folder() + '/' + self.CONFIG_FILE_NAME
            # self.Server.logger.info(configPath)
            with open(configPath, 'r') as configFile:
                return json.load(configFile)
        except (FileNotFoundError, json.JSONDecodeError):
            self.Server.logger.warning('Failed to load the config file.')
            return


    def UpdateConfigFile(self, configDict):
        pass


    # 包括 配置的读取 和 任务的加载
    def SchedulerStart(self):
        config = self.ConfigLoad()
        if not config:
            return
        
        if not config[self.TAG_ENABLED]:
            self.Server.logger.info('L Scheduler is disabled by Config!')
            return

        count = 0
        for task in config.get(self.TAG_TASKS, []):
            enabled = task[self.TAG_ENABLED]
            name = task[self.TAG_TASK_NAME]
            commands = task[self.TAG_TASK_COMMANDS]
            crontab = task[self.TAG_TASK_CRONTAB]

            if not enabled:
                continue

            if name and crontab:
                trigger = CronTrigger.from_crontab(crontab)
                self.Scheduler.add_job(
                    func=self.ExecuteCommands,
                    id=name,
                    args=[commands,],
                    trigger=trigger
                )
                count += 1
                self.Server.logger.info(f'Loaded task: {name}')
            else:
                self.Server.logger.error(f"Invalid task configuration: {task}")

        self.Scheduler.start()
        self.schedulerStarted = True
        self.Server.logger.info(f'Loaded {count} task(s). Scheduler started successfully!')


    ''' # By ChatGPT
    def schedule_task(self.Server, task_name, command):
        global scheduler

        # Remove existing task with the same name
        if scheduler.get_job(task_name):
            scheduler.remove_job(task_name)

        # Get cron expression from user input
        cron_expression = input('Enter cron expression for the task (e.g., "0 0 * * *"): ')

        # Save task to config
        config = load_config()
        config['tasks'][task_name] = {
            'cron_expression': cron_expression,
            'command': command,
        }
        save_config(config)

        # Schedule the task
        trigger = CronTrigger.from_crontab(cron_expression)
        scheduler.add_job(
            schedule_task_callback, 
            trigger=trigger, 
            args=[self.Server, 
            task_name, command], 
            id=task_name
        )
        self.Server.logger.info(f'Task "{task_name}" scheduled successfully!')


    def schedule_task_callback(self.Server, task_name, command):
        self.Server.execute(command)
        self.Server.logger.info(f'Task "{task_name}" executed successfully!')
    '''


    def TaskAdd(self, name, enabled, commands, crontab):
        pass


    def TaskCancel(self, task_name):
        # Remove task from Scheduler and config
        if self.Scheduler.get_job(task_name):
            self.Scheduler.remove_job(task_name)
            self.Server.logger.info(f'Task "{task_name}" canceled successfully! (from Scheduler)')

        config = self.ConfigLoad()
        if task_name in config['tasks']:
            del config['tasks'][task_name]
            self.ConfigUpdate(config)
            self.Server.logger.info(f'Task "{task_name}" canceled successfully! (in config file)')
        else:
            self.Server.logger.info(f'Task "{task_name}" not found! (in config file)')


    def ExecuteCommands(self, commands: list):
        for command in commands:
            commandLine = command[self.TAG_COMMAND_LINE]
            if command[self.TAG_COMMAND_TYPE] == self.TAG_COMMAND_TYPE_MCDR:
                self.Server.execute_command(commandLine)
            elif command[self.TAG_COMMAND_TYPE] == self.TAG_COMMAND_TYPE_MC_SERVER:
                self.Server.execute(commandLine)
            else:
                self.Server.logger.error(f'Wrong command type: {command}')
                self.Server.logger.error('Task stopped!')
                return


    def SchedulerStop(self):
        if self.schedulerStarted == True:
            self.Scheduler.shutdown(wait=False)
            self.Scheduler = None
            self.Server.logger.info('Scheduler stopped!')
        else:
            self.Server.logger.info("Scheduler isn't running.")