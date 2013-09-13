typedef list<pair<string, list<string> > > taskQueue_t;
typedef list<map<string, string> > scannerQueue_t;
typedef map<string, map<string, string> > taskStatusInfo_t;

taskQueue_t taskQueue;
scannerQueue_t scannerQueue;


int main()
{
while(true)
{
	scannerQueue.clear();
	shared_ptr<CWatcherHandler> watcherHandlerPtr = CWatcherHandler::getInstance();
	scannerQueue = watcherHandlerPtr->getScannerPayload();
	sort(scannerQueue.begin(), scannerQueue.end(), sortByPayload);
     
	FD_ZERO(&fds);
	FD_SET(worker_fd, &fds);
	if(select(worker_fd+1, &fds, NULL, NULL, &timeout))
    {
		if(FD_ISSET(worker_fd, &fds))
		{
			pair<string, list<string> > taskInfo = receive_from_worker(worker_fd);
			taskQueue.push_back(taskInfo);
		}
	}

    taskQueue_t::iterator front = taskQueue.begin();
    string key = front->first;  // taskId#instanceId
    list<string> * hosts = &front->second; // use ptr inorder to change the value
    string taskId = getTaskId(key);
    string instanceId = getInstanceId(key);
    
    int hostsCount = hosts->size();
    int freeScannerCount = getFreeScannerCount(scannerQueue);
    if(freeScannerCount > 0)
    {
		taskStatusInfo_t taskStatusInfo;  // used to save to HBase
        if(hostsCount >= K * freeScannerCount)
        {
            for(int i=0; i<freeScannerCount; i++)
            {
				map<string, string> tmp = scannerQueue.front();
                string scannerId = tmp["scanner"];
                list<string> hosts_to_assign;
                map<string, string> taskDescription;
				map<string, string> scannerStatus;
                for(int j=0; j<K; j++)
                {   
                    hosts_to_assign.push_back(hosts->front());
					scannerStatus.insert(make_pair(hosts->front(), "waiting"));
                    hosts->pop_front();
                }
				taskDescription["taskId"] = taskId;
				taskDescription["instanceId"] = instanceId;
				taskDescription["scannerId"] = scannerId;
                taskDescription["hosts"] = toJsonString(hosts_to_assign);
				// send to executor
				shared_ptr<CExecutorHandler> executorHandlerPtr = CExecutorHandler::getInstance();
				executorHandlerPtr->startTask(taskDescription);
				
				taskStatusInfo[scannerId] = scannerStatus;
				scannerQueue.pop_front();
			}
        }
		else
		{
			double totalPayload = 0;
			scannerQueue_t::iterator p = scannerQueue.begin();
			for(int i=0; i<freeScannerCount; i++,p++)
				totalPayload += string2double((*p)["payload"]);

			for(int i=0; i<freeScannerCount; i++)
			{
				map<string, string> tmp = scannerQueue.front();
				string scannerId = tmp["scanner"];
				double payload = string2double(tmp["payload"]);
				hostsCount = hosts.size();
				int planHostCount = hostsCount * payload / totalPayload;

				list<string> hosts_to_assign;
				map<string, string> taskDescription;
				map<string, string> scannerStatus;
				for(int j=0; j<(planHostCount >= K ? K : planHostCount); j++)
				{
					hosts_to_assign.push_back(hosts->front());
					scannerStatus.insert(make_pair(hosts->front(), "waiting"));
					hosts->pop_front();
				}
				taskDescription["taskId"] = taskId;
				taskDescription["instanceId"] = instanceId;
				taskDescription["scannerId"] = scannerId;
				taskDescription["hosts"] = toJsonString(hosts_to_assign);
				// send to executor
				shared_ptr<CExecutorHandler> executorHandler(CExecutorHandler::getInstance());
				executorHandler->startTask(taskDescription);
				
				taskStatusInfo[scannerId] = scannerStatus;
				scannerQueue.pop_front();
				totalPayload -= payload;
			}
		}

		// get original taskInfo from HBase
		shared_ptr<HClient> hc = HClient()::getInstance("task_status_info");
		string primaryKey = getPrimaryKey(taskId);
		string taskInfo;
		hc->readCell(primaryKey, instanceId, taskInfo);
		taskStatusInfo_t src = fromJsonString(taskInfo);
		// merge src and taskStatusInfo
		mergeMap(src, taskStatusInfo);
		// save to HBase
		hc->writeCell(primaryKey, instanceId, toJsonString(src));
    }

	// TODO: failover(step 7)
    scannerQueue_t::iterator iter;
    for(iter = scannerQueue.begin(); iter != scannerQueue.end(); iter++)
    {
        if("0" == (*iter)["islive"] && "1" == (*iter)["tasknum"])
        {
            string scannerId = (*iter)["scannerId"];
            string taskId2 = (*iter)["taskId"];  // different from the above
            string instanceId2 = (*iter)["instanceId"]; // different from the above
            string primaryKey = getPrimaryKey(taskId2);
            string result;
            shared_ptr<HClient> hc = HClient()::getInstance("task_status_info");
            hc->readCell(primaryKey, instanceId2, result);
            taskStatusInfo_t taskStatusInfo = fromJsonString(result);

            taskStatusInfo_t::iterator fPtr = taskStatusInfo.find(scannerId);
            if(fPtr != taskStatusInfo.end())
            {
                list<string> hosts_to_assign;
                map<string, string> vhosts = fPtr->second;
                for(map<string, string>::iterator it = vhosts.begin(); it != vhosts.end(); it++)
                {
                    if(("waiting" == it->second) || ("running" == it->second))
                        hosts_to_assign.push_back(it->first);
                }
                taskQueue_t::iterator tqIter = findPairKey(taskQueue, taskId2 + "#" + instanceId2);
                if(tqIter != taskQueue.end())
                    mergeList(tqIter->second, hosts_to_assign);
                else
                    taskQueue.push_front(make_pair(taskId2 + "#" + instanceId2, hosts_to_assign)); // priority highest
            }
        }
    }
}
