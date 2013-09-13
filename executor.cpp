STATUS startTask(map<string, string> taskDescription)
{
    string taskId = taskDescription["taskId"];
    string primaryKey = getPrimaryKey(taskId);
    string plugins;
    shared_ptr<HClient> hc = HClient()::getInstance("task_status_info");
    try
    {
        hc->readCell(primaryKey, TASK_META_INFO_PLUGINS, plugins);
        taskDescription["plugins"] = plugins;

        shared_ptr<CScannerHandler> scannerHandlerPtr = CScannerHandler::getInstance();
        scannerHandlerPtr->startTask(taskDescription);
        return SUCCESS;
    }
    catch(HBaseException)
    {
        return FAIL;
    }
    catch(ScannerException)
    {
        return FAIL;
    }
}
