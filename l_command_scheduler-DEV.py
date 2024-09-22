from mcdreforged.api.all import *
from mcdreforged.api.command import SimpleCommandBuilder, Integer, Text, GreedyText, Literal
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time # 用于配置文件备份命名



PLUGIN_METADATA = {
    "id": "l_command_scheduler",
    "version": "0.0.9",
    "name": "L-CommandScheduler-DEV",
    "link": "https://github.com/lllrmm/L-CommandScheduler",
    "description": {
        "en_us": "A MCDR plugin that schedules commands to run based on crontab.",
        "zh_cn": "一个基于crontab自动执行指令（组）的MCDR插件。"
    },
    "author": [
        "lllrmmGP"
    ],
}


SchedulerMng = None
CommandBuilder = SimpleCommandBuilder()


def on_load(Server: PluginServerInterface, prev_module):
    global SchedulerMng
    Server.logger.info('Plugin loading now...')
    SchedulerMng = Lscheduler(Server)
    SchedulerMng.ConfigLoad()
    register_help_message(Server)
    SchedulerMng.SchedulerStart()


def on_unload(Server: PluginServerInterface):
    global SchedulerMng
    SchedulerMng.SchedulerShutdown()
    SchedulerMng = None


def register_help_message(Server: PluginServerInterface):
    help_messages = (
        ('!!cmds', PLUGIN_METADATA['description']['zh_cn']),
    )

    for command, description in help_messages:
        Server.register_help_message(command, description)



class permCheck:
    '''权限检查 装饰器'''
    def __init__(self, level):
        self.level = level

    def __call__(self, func):
        def warpper(funcSelf, Source: ConsoleCommandSource, args):
            Server = Source.get_server()
            if Source.has_permission(self.level):
                return func(funcSelf, Source, args)
            else:
                Server.logger.warning(f'Permission denied! Command: "{Source.get_info().content}"')
                Source.reply('You do NOT have permission to execute this command!')

        return warpper



class Task:
    TAG_TASK_NAME = 'name'
    TAG_TASK_COMMANDS = 'commands'
    TAG_TASK_CRONTAB = 'crontab'
    TAG_COMMAND_TYPE = 'type'
    TAG_COMMAND_LINE = 'line'
    TAG_COMMAND_TYPE_MCDR = 'MCDR'
    TAG_COMMAND_TYPE_MC_SERVER = 'MCserver'


    def __init__(self, L):
        self.L: Lscheduler = L
        self.invalid = False


    def Execute(self):
        if self.invalid:
            self.L.Server.logger.error(f'Task "{self.name}" would not not execute: Configuration invalid!')
            return

        self.L.Server.logger.info(f'Executing Task "{self.name}" now...')

        count = 0
        for command in self.commands:
            commandLine = command[self.TAG_COMMAND_LINE]
            commandType = command[self.TAG_COMMAND_TYPE]

            if commandType == self.TAG_COMMAND_TYPE_MCDR:
                self.L.Server.execute_command(commandLine)
                count += 1

            elif commandType == self.TAG_COMMAND_TYPE_MC_SERVER:
                self.L.Server.execute(commandLine)
                count += 1

            else:
                self.L.Server.logger.error(f'Wrong command type: {command}')
                self.L.Server.logger.error(f'Task "{self.name}" stopped!')
                break

        self.L.Server.logger.info(f'{count} line(s) for Task "{self.name}" executed!')


    def Pack(self):
        data = {}
        data[self.TAG_TASK_NAME] = self.name
        data[self.L.TAG_ENABLED] = self.enabled
        data[self.TAG_TASK_COMMANDS] = self.commands
        data[self.TAG_TASK_CRONTAB] = self.crontab
        
        return data


    def UnPack(self, taskData: dict):
        '''要确保所有参数合法'''
        try:
            self.enabled = taskData[self.L.TAG_ENABLED] # 若为true则均被添加到Scheduler
            self.name: str = taskData[self.TAG_TASK_NAME]
            self.commands = taskData[self.TAG_TASK_COMMANDS]
            self.crontab = taskData[self.TAG_TASK_CRONTAB]

            if not (self.name and self.crontab):
                self.L.Server.logger.error(f"Invalid Task configuration: {taskData}.")
                raise
            if (' ' in self.name):
                self.L.Server.logger.error(f"Invalid Task name: {self.name}. Must not include spaces.")
                raise
        except:
            self.invalid = True
            return 1

        self.trigger = CronTrigger.from_crontab(self.crontab)



class Lscheduler:
    CONFIG_FILE_NAME = 'l_command_scheduler_config.json'
    CONFIG_FORMAT = 'json'
    TAG_ENABLED = 'enabled'
    TAG_TASKS = 'tasks'
    ERROR_NOT_FOUND = 1
    RELOAD_COMMAND = '!!MCDR plugin reload ' + PLUGIN_METADATA['id']
    PERM_LEVEL = 3

    DEFAULT_CONFIG = {
        TAG_ENABLED: True,
        TAG_TASKS: [
            {
                Task.TAG_TASK_NAME: "StopRemind",
                TAG_ENABLED: False,
                Task.TAG_TASK_COMMANDS: [
                    {Task.TAG_COMMAND_TYPE: "MCserver",
                    Task.TAG_COMMAND_LINE: "say 服务器将在23:00关闭"}
                ],
                Task.TAG_TASK_CRONTAB: "50-59 14 * * *"
            },
            {
                Task.TAG_TASK_NAME: "AutoStopServer",
                TAG_ENABLED: False,
                Task.TAG_TASK_COMMANDS: [
                    {Task.TAG_COMMAND_TYPE: "MCserver",
                    Task.TAG_COMMAND_LINE: "say 服务器关闭!"},
                    {Task.TAG_COMMAND_TYPE: "MCDR",
                    Task.TAG_COMMAND_LINE: "!!MCDR server stop"}
                ],
                Task.TAG_TASK_CRONTAB: "0 15 * * *"
            }
        ]
    }



    class getTaskName:
        '''参数处理之 获取taskName 装饰器'''

        def __init__(self, func):
            self.func = func

        def __call__(self, funcSelf, Source, args):
            # print(args)
            taskName = args['task_name']
            return self.func(funcSelf, Source, taskName)



    def __init__(self, Server: PluginServerInterface):
        self.Server = Server
        self.Scheduler = BackgroundScheduler()
        self.Scheduler.add_executor('threadpool')
        self.enabled = True # default
        self.tasks: list[Task] = [] # 要确保添加到其中的Task均合法
        self.paused = False


    def RegisterCommands(self):
        global CommandBuilder

        CommandBuilder.command('!!cmds',                          self.Info)
        CommandBuilder.command('!!cmds info',                     self.Info)
        CommandBuilder.command('!!cmds pause',                    self.SchedulerPause)
        CommandBuilder.command('!!cmds resume',                   self.SchedulerResume)
        CommandBuilder.command('!!cmds exec <task_name>',         self.ExecuteTaskManually)
        CommandBuilder.command('!!cmds reload',                   self.ReloadPlug)

        CommandBuilder.arg('task_name', Text)

        CommandBuilder.register(self.Server)

        # CommandBuilder.command('!!cmds task list',                SchedulerMng.InfoTasksList) # x
        # CommandBuilder.command('!!cmds task info    <task_name>', SchedulerMng.InfoTask) # x
        # CommandBuilder.command('!!cmds task enable  <task_name>', SchedulerMng.TaskEnable) # x
        # CommandBuilder.command('!!cmds task disable <task_name>', SchedulerMng.TaskDisable) # x
        # CommandBuilder.command('!!cmds task delete  <task_name>', SchedulerMng.TaskDelete) # x
        # CommandBuilder.command('!!cmds config write',             SchedulerMng.ConfigWrite) # 待完善，暂不开放



    def ConfigLoad(self):
        '''加载配置文件，仅限插件加载时运行一次（自动备份老配置文件）'''
        self.Server.logger.info('Config loading now...')

        config = self.Server.load_config_simple(
            self.CONFIG_FILE_NAME, 
            file_format=self.CONFIG_FORMAT, 
            failure_policy='raise', 
            default_config=self.DEFAULT_CONFIG
        )
        # json.JSONDecodeError

        self.enabled = config[self.TAG_ENABLED]
        if not self.enabled:
            self.Server.logger.info(f'{PLUGIN_METADATA["name"]} is disabled by Config!')
            return 1
        
        self.RegisterCommands() # 注册!!cmds

        for taskData in config.get(self.TAG_TASKS, []):
            self.TaskLoad(taskData)

        self.Server.logger.info(f'Loaded {len(self.tasks)} Task(s). Config loading finished!')


    @permCheck(PERM_LEVEL)
    def ConfigWrite(self, Source: ConsoleCommandSource, args):
        '''
        将变更写入配置文件（自动备份老配置文件、就结果回复用户、log）
        '''
        config = {}
        tasks = [TaskObj.Pack() for TaskObj in self.tasks]

        config[self.TAG_ENABLED] = self.enabled
        config[self.TAG_TASKS] = tasks
        
        self.Server.save_config_simple(
            config,
            file_name=self.CONFIG_FILE_NAME,
            file_format=self.CONFIG_FORMAT
        )


    def TaskLoad(self, taskData):
        '''加载 taskData，已过滤不合法的Task（不添加到self.tasks中）'''
        TaskObj = Task(L=self)
        sta = TaskObj.UnPack(taskData)
        if sta == 1: # 过滤不合法Task
            return sta

        self.tasks.append(TaskObj)
        if TaskObj.enabled:
            self.Server.logger.info(f"""Loaded    Task "{TaskObj.name}" (Enabled.)""")
            self._TaskSchedule(TaskObj)
        else:
            self.Server.logger.info(f"""Loaded    Task "{TaskObj.name}" (Disabled. Won't be scheduled.)""")


    def _TaskSchedule(self, TaskObj):
        '''将Task对象添加到Scheduler（内部）。Task需要为enabled，否则报错。'''
        if TaskObj.enabled:
            self.Scheduler.add_job(
                func=TaskObj.Execute,
                id=TaskObj.name,
                trigger=TaskObj.trigger
            )
            self.Server.logger.info(f'Scheduled Task "{TaskObj.name}"')
        else:
            raise # 


    @permCheck(PERM_LEVEL)
    @getTaskName
    def TaskDelete(self, Source: ConsoleCommandSource, taskName):
        '''删除Task（就结果回复用户、log）'''
        try:
            TaskObj = self._GetTaskByName(taskName)
        except NotFoundError:
            return

        try:
            self.TaskDisable(TaskObj.name)
        except:
            pass
        finally:
            self.tasks.remove(TaskObj)


    @permCheck(PERM_LEVEL)
    @getTaskName
    def TaskEnable(self, Source: ConsoleCommandSource, taskName):
        '''启用已有Task（判断状态并回复用户、log）'''
        try:
            TaskObj = self._GetTaskByName(taskName)
        except NotFoundError:
            return

        # 判断是否已为Enabled
        if TaskObj.enabled == False:
            TaskObj.enabled = True
            self._TaskSchedule(TaskObj)
        elif TaskObj.enabled == True:
            pass #already Enabled
        else:
            raise


    @permCheck(PERM_LEVEL)
    @getTaskName
    def TaskDisable(self, Source: ConsoleCommandSource, taskName):
        '''禁用已有Task（判断状态并回复用户、log）'''
        try:
            TaskObj = self._GetTaskByName(taskName)
        except NotFoundError:
            return
        
        # 判断是否已为Disabled
        if TaskObj.enabled == True:
            self.Scheduler.remove_job(TaskObj.name)
            TaskObj.enabled = False
            # logger
        elif TaskObj.enabled == False:
            pass #already Disabled
        else:
            raise


    def _GetTaskByName(self, taskName) -> Task:
        '''[内部] 用于插件内部获取Task对象'''
        for TaskObj in self.tasks:
            if TaskObj.name == taskName:
                return TaskObj

        raise NotFoundError
        # self.Server.logger.error("")# not found


    @permCheck(PERM_LEVEL)
    def Info(self, Source: ConsoleCommandSource, args):
        '''展示插件基础信息（未完成）'''

        taskListInfo = ''
        for TaskObj in self.tasks:
            taskInfo = f'   {TaskObj.name}  [cron="{TaskObj.crontab}" enabled={TaskObj.enabled}]\n'
            taskListInfo = taskListInfo + taskInfo

        info = f'''
L-CommandScheduler (version: {PLUGIN_METADATA['version']})
   status: paused={self.paused}
==Available commands==
   !!cmds info
   !!cmds pause
   !!cmds resume
   !!cmds exec <task_name>
   !!cmds reload
==Loaded Task List==
{taskListInfo}
'''
        Source.reply(info)


    @permCheck(PERM_LEVEL)
    def InfoTasksList(self, Source: ConsoleCommandSource, args):
        '''列出所有任务（全未完成）'''
        pass


    @permCheck(PERM_LEVEL)
    @getTaskName
    def InfoTask(self, Source: ConsoleCommandSource, taskName):
        '''用于单个已有任务信息展示（打印）（全未完成）'''
        try:
            TaskObj = self._GetTaskByName(taskName)
        except NotFoundError:
            return
        infoPack = TaskObj.Pack()
        pass # output


    @permCheck(PERM_LEVEL)
    @getTaskName
    def ExecuteTaskManually(self, Source: ConsoleCommandSource, taskName):
        '''手动运行任务（不论是否enabled）若为禁用，则提醒。'''
        try:
            TaskObj = self._GetTaskByName(taskName)
        except NotFoundError:
            self.Server.logger.error(f'Task "{taskName}" not exist!')
            Source.reply(f'Task "{taskName}" dose NOT exist!')
            return

        if TaskObj.enabled != True:
            self.Server.logger.warning(f'Manually execute Task "{taskName}"(disabled!)!')
            Source.reply(f'Executing Task "{taskName}"(disabled!) manually now...')
        else:
            self.Server.logger.info(f'Manually execute Task "{taskName}"(enabled.)!')
            Source.reply(f'Executing Task "{taskName}"(enabled.) manually now...')
        TaskObj.Execute()


    def SchedulerStart(self):
        '''仅限插件加载时运行一次'''
        if not self.enabled:
            self.Server.logger.warn(f'If the plugin is disabled, the Scheduler would not start!')
            return

        self.Server.logger.info(f'Scheduler starting now...')

        self.Scheduler.start()
        self.schedulerStarted = True
        scheduledTasksNum = len(self.Scheduler.get_jobs())
        self.Server.logger.info(f'Scheduled(Enabled) {scheduledTasksNum} Task(s). Scheduler started successfully!')


    @permCheck(PERM_LEVEL)
    def SchedulerPause(self, Source: ConsoleCommandSource, args):
        '''暂停Scheduler'''
        if self.Scheduler.state == 2:
            # self.Server.logger.warning('Scheduler is already paused!')
            Source.reply('Scheduler is already paused!')
        else:
            self.Scheduler.pause()
            self.paused = True
            self.Server.logger.info('Scheduler paused!')
            Source.reply('Scheduler is paused now!')


    @permCheck(PERM_LEVEL)
    def SchedulerResume(self, Source: ConsoleCommandSource, args):
        '''继续Scheduler'''
        if self.Scheduler.state == 1:
            # self.Server.logger.warning('Scheduler is already running!')
            Source.reply('Scheduler is already running!')
        else:
            self.Scheduler.resume()
            self.paused = False
            self.Server.logger.info('Scheduler resumed.')
            Source.reply('Scheduler is resumed now!')


    def SchedulerShutdown(self):
        '''仅限卸载插件时运行一次'''
        if self.Scheduler.running == True:
            self.Scheduler.shutdown(wait=False)
            self.Server.logger.info('Scheduler stopped!')
        else:
            self.Server.logger.info("Scheduler isn't running.")
        self.Scheduler = None


    @permCheck(PERM_LEVEL)
    def ReloadPlug(self, Source: ConsoleCommandSource, args):
        '''调用MCDR指令实现自身重载'''
        Source.reply("Reloading L-CommandScheduler, using MCDR's command.")
        self.Server.logger.info("Reloading L-CommandScheduler, using MCDR's command.")
        self.Server.execute_command(self.RELOAD_COMMAND)




class NotFoundError(Exception):
    pass

