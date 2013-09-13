TASK_META_INFO_HOSTS = "info:hosts"
TASK_META_INFO_PLUGINS = "info:plugins"

STATUS saveTaskInfo(string hosts, string plugins, ...)
{
    string taskId = generateTaskId();
    shared_ptr<HClient> hc(HClient()::getInstance("task_meta_info"));
    string primaryKey = getPrimaryKey(taskId);
    try
    {   
        hc->writeCell(primaryKey, TASK_META_INFO_HOSTS, hosts);
        hc->writeCell(primaryKey, TASK_META_INFO_PLUGINS, plugins);
        return SUCCESS; 
    }   
    catch(HBaseException)
    {   
        return FAIL;
    }   
}

STATUS startTask(string taskId)
{
    shared_ptr<HClient> hc(HClient()::getInstance("task_meta_info"));
    string primaryKey = getPrimaryKey(taskId);
    string hosts;
    list<string> vhosts;
    try 
    {   
        hc->readCell(primaryKey, TASK_META_INFO_HOSTS, hosts);
        vhosts = fromJsonString(hosts);
        pair<string, list<string> > taskInfo;
        string instanceId = generateInstanceId();
        string key = taskId + "#" + instancdId;
        taskInfo = make_pair(key, vhosts);
        shared_ptr<CSchedulerHandler> schedulerHandler(CSchedulerHandler::getInstance());
        schedulerHandler->startTask(taskInfo);
        return SUCCESS;
    }   
    catch(HBaseException)
    {   
        return FAIL;
    }   
    catch(SchedulerException)
    {   
        return FAIL;
    }   
}
