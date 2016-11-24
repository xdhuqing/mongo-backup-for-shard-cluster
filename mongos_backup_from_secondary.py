#!/usr/bin/env python

'''
function description:
1-parse config file;
2-connect to router, close 'balancer', then forbid writing;
3-config server: full backup everyday;
4-each shard: full backup every monday, and inc backup everyday;
5-connect to router, open 'balancer', then permit writing.
author: qing.hu
date: 2016-10-24
version: v1.0.0
'''

import os
import sys
import pymongo
import ConfigParser
import commands
from time import sleep
from pyasn1.compat.octets import null

'''
description: config class, parse and check config parameters.
'''
class Config:
    '''
    '''
    def __init__(self,path):
        self.parser = ConfigParser.ConfigParser()
        self.path=path
        self.mongo_bin_dir = ''
        self.full_backup_dir = ''
        self.inc_backup_dir = ''
        self.backup_start_date = ''
        
        self.mongos_ip = ''
        self.mongos_port = -1
        
        self.config_ip = ''
        self.config_port = -1
        
        self.parseConfig()
        
    def parseConfig(self):
        self.parser.read(self.path)
        
        self.mongo_bin_dir = self.parser.get('base-options', 'mongo_bin_dir')
        self.full_backup_dir = self.parser.get('base-options', 'full_backup_dir')
        self.inc_backup_dir = self.parser.get('base-options', 'inc_backup_dir')
        self.backup_start_date = self.parser.get('base-options', 'backup_start_date')
        
        self.mongos_ip = self.parser.get('mongos-options', 'mongos_ip')
        self.mongos_port = int(self.parser.get('mongos-options', 'mongos_port'))
        
        self.config_ip = self.parser.get('config-server-options', 'config_ip')
        self.config_port = self.parser.get('config-server-options', 'config_port')
 
    def __str__(self):
        return 'mongo_bin_dir is: '+self.mongo_bin_dir+'\n'     \
            +'full_backup_dir is: '+self.full_backup_dir+'\n'   \
            +'inc_backup_dir is: '+self.inc_backup_dir+'\n'     \
            +'backup_start_date is: '+self.backup_start_date+'\n'     \
            +'mongos_ip is: '+self.mongos_ip+'\n'               \
            +'mongos_port is: '+str(self.mongos_port)+'\n'      \
            +'config_ip is: '+self.config_ip+'\n'               \
            +'config_port is: '+str(self.config_port)+'\n'

'''
description: Backup class, execute backup command.
'''           
class Backup:
    def __init__(self,config):
        self.backupFinished = False
        self.config = config
        self.client = pymongo.MongoClient(self.config.mongos_ip,int(self.config.mongos_port))
        self.shard_info = []
        #for each shard, only one second used
        self.second_node_used = {}
    
    def getShardInfo(self):
        collection = self.client.get_database("config").get_collection("shards")
        shard_doc = collection.find()
        if shard_doc:
            for shard in shard_doc:
                tempDict = {}
                tempDict['name'] = shard.get('_id') 
                host = shard.get('host')
                if str(host).find('/') != -1:
                    host = shard.get('host').split('/')[1]
                tempDict['host'] = host 
                self.shard_info.append(tempDict)
            return True   
        
    def prepareForBackup(self):
        #close balancer
        config_col = self.client.get_database("config").get_collection("settings")
        config_col.update({'_id':'balancer'},{'$set':{'stopped':True}}, True)
        doc = config_col.find({'_id':'balancer'})
        if not doc:
            print "closing balancer fails!"
            return False
        if doc and str(doc).lower().find('true'):
            print "balancer is stopped!"
            
        #forbid each shard's one second from writing
        for shard in self.shard_info:
            second_node = []
            second_node = self.getSecondary(shard['host'])
            if second_node.__len__() == 0:
                 return False
            self.second_node_used[shard['name']] = second_node[0]
            cmd_forbid_write = self.config.mongo_bin_dir + "/mongo "+  \
                          str(second_node[0]) + "/admin "     \
                          " --quiet " +              \
                          " --eval 'rs.slaveOk();db.fsyncLock();db.currentOp();' "+\
                          "| grep 'fsyncLock\\>' " +\
                          "| grep true"
            status, isLocked = commands.getstatusoutput(cmd_forbid_write)
            if status != 0 or not isLocked:
                print 'fsncLock '+ second_node[0] + ' fail! exist now!'
                return False    
        return True         
            
    def recoverMongosAfterBackup(self):
        #unlock each shard's second
        for second in self.second_node_used.itervalues():
            print second
            cmd_permit_write = self.config.mongo_bin_dir + "/mongo "+  \
                               str(second) + "/admin "   \
                               " --quiet " +                \
                               " --eval 'db.fsyncUnlock()'"+\
                               " | egrep 'unlock completed|not locked' " 
            status, unLocked = commands.getstatusoutput(cmd_permit_write)
            if status != 0 or not unLocked:
                print 'fsncUnlock '+ second + ' fail!'
        
        #open balancer    
        config_col = self.client.get_database("config").get_collection("settings")
        config_col.update({'_id':'balancer'},{'$set':{'stopped':False}}, True)
        doc = config_col.find({'_id':'balancer'})
        if not doc:
            print "starting balancer fails!"
            return False
        if doc and str(doc).lower().find('false'):
            print "balancer is started!"
        return True

    def backupConfig(self):
        if check_dir(self.config.full_backup_dir):
            status, date = commands.getstatusoutput("date +%Y-%m-%d")
            backup_dir = self.config.full_backup_dir + '/' + date
            commands.getstatusoutput("mkdir -p "+backup_dir)
            cmd_line = self.config.mongo_bin_dir + "/mongodump"    \
                      " --host " + self.config.config_ip +         \
                      " --port " + str(self.config.config_port) +  \
                      " --out "  + backup_dir
            status, output = commands.getstatusoutput(cmd_line)
            if status != 0 or not output:
                print "backup config " + str(config_ip) + ':' + str(config_port) + " failed!"
                return False
            print 'finished backup config ' + str(config_ip) + ':' + str(config_port) + '.'
            return True
  
    def fullBackupShard(self):
         dir = self.config.full_backup_dir
         if check_dir(dir):
             for shard in self.shard_info:
                 #create new dir for each shard
                status, date = commands.getstatusoutput("date +%Y-%m-%d")
                backup_dir = dir + '/' + date+ '/' + shard['name']
                commands.getstatusoutput("mkdir -p "+backup_dir)
                last_oplog_dir = dir + '/oplog_json/'+ shard['name']
                commands.getstatusoutput("mkdir -p "+last_oplog_dir)
               
                #get the second node of this shard
                second = self.second_node_used[shard['name']]
                if second == null:
                    return False
                
                #find the last oplog record
                cmd_last_oplog_record = self.config.mongo_bin_dir + "/mongo "+  \
                          str(second) + "/local "     \
                          " --quiet " +              \
                          " --eval 'db.oplog.rs.find({\"ts\":{\"$gte\":new Timestamp(" +\
                          self.config.backup_start_date + \
                          ",1)}}).sort({\"ts\":-1}).limit(1)' "+\
                          "| grep Timestamp > status-temp.json"  
                commands.getstatusoutput(cmd_last_oplog_record)
                
                #do dump
                cmd_dump = self.config.mongo_bin_dir + "/mongodump " + \
                          " --host " + str(second) +         \
                          " --out "  + backup_dir
                status, output = commands.getstatusoutput(cmd_dump)
                
                #save the last oplog record
                commands.getstatusoutput('mv -f status-temp.json '+last_oplog_dir+'/status.json')
               
                if status != 0 or not output:
                    print "full backup shard " + str(second) + " failed!"
                    return False
                print "finished full backup " + str(second) + "."
                return True
            
    def incBackupShard(self):
         dir = self.config.inc_backup_dir
         full_dir = self.config.full_backup_dir
         if check_dir(dir) and check_dir(full_dir):
            for shard in self.shard_info:
                
                last_oplog_dir = full_dir + '/oplog_json/'+ shard['name']
               
                #create new dir for each shard
                status, date = commands.getstatusoutput("date +%Y-%m-%d")
                backup_dir = dir + '/' + date+ '/' + shard['name']
                commands.getstatusoutput("mkdir -p "+backup_dir)
                
                #get the second node of this shard
                second = self.second_node_used[shard['name']]
                if second == null:
                    print 'can not find secondary for ' + shard['name'] + '.'
                    return False
               
                #get timestamp
                cmd_get_ts = "cat "+last_oplog_dir+"/status.json "+\
                            r"| awk -F ',' '{print $1}' | awk -F '(' '{print $2}'"
                status, ts = commands.getstatusoutput(cmd_get_ts)
                cmd_get_inc = "cat " + last_oplog_dir + "/status.json "+\
                            r"| awk -F ',' '{print $2}' | awk -F ')' '{print $1}'"
                status, inc = commands.getstatusoutput(cmd_get_inc)
                
                #find the last oplog record
                cmd_last_oplog_record = self.config.mongo_bin_dir + "/mongo "+  \
                          str(second) + "/local "     \
                          " --quiet " +              \
                          " --eval 'db.oplog.rs.find({\"ts\":{\"$gte\": new Timestamp(" +\
                          str(ts)  +','+ str(inc) +" )}}).sort({\"ts\":-1}).limit(1)' "+\
                          "| grep Timestamp > status-temp.json"  
                commands.getstatusoutput(cmd_last_oplog_record)
                
                #do dump
                query = '\'{\"ts\":{\"$gte\":Timestamp('+ str(ts)  +','+ str(inc) +' )}}\''
                cmd_dump = "mongodump --host " + second + " --out " + backup_dir + \
                           " --db local --collection oplog.rs --query " + query
                status, output = commands.getstatusoutput(cmd_dump)
               
                #save the last oplog record
                commands.getstatusoutput('mv -f status-temp.json '+last_oplog_dir+'/status.json')
                
                #save oplog.bson, and delete local dir
                commands.getstatusoutput('mv -f '+backup_dir+'/local/oplog.rs.bson ' + backup_dir+'/oplog.bson')
                commands.getstatusoutput('rm -rf '+backup_dir+'/local')
                
                if status != 0 or not output:
                    print "inc backup shard " + str(second) + " failed!"
                    return False
                print "finished inc backup " + str(second) + "."
                return True

    def getSecondary(self, shard):
        secondaryList = []
        print shard + ':getting secondaries ... '
        shard_client = pymongo.MongoClient(shard)
        
        #get server info first, otherwise shard_client.secondaries return nothing
        shard_client.server_info()
        for second in shard_client.secondaries:
            secondaryList.append(second[0]+':'+str(second[1]))
        return secondaryList
    
'''
description: function, check if the given directory exists.
return: True-if exists, False-if not.
'''    
def check_dir(dir):
    if os.path.isdir(dir):
        return True
    else:
        print str(dir)+' does not exists!'
        return False

'''
description: replace the parameter backup_start_date in config file(mongos_backup.conf) with today.
            This parameter indicates that the next full backup start-date.
return: None.
'''    
def changeConfBackStartDate():
    today = datetime.date.today()
    properties = open('mongos_backup.conf','rb+')
    lines = properties.readlines()
    d=""
    for line in lines:
        c = line
        if "backup_start_date" in c:
            c = line.replace(str(line).split('=')[1], str(today) + os.linesep)
        d += c
    properties.seek(0)     
    properties.truncate()
    properties.write(d)
    properties.close() 

  
'''
description: function, control the whole process of restoring.
return: None.
'''            
def Launcher(path, backType):
    config = Config(path)
    print(config)
    
    back = Backup(config)
    if back.getShardInfo():
        print "get shard info successfully!"
        if back.prepareForBackup():
            print "now, ready for backup!"
            if backType == 'full':
                if back.backupConfig() and ack.fullBackupShard() and back.incBackupShard():
                    changeConfBackStartDate()
            elif backType == 'inc':
                back.backupConfig()
                back.incBackupShard()
            else:
                print 'unknown parameter, exist!'
                back.recoverMongosAfterBackup()
                return False
         
        #recover mongos
        back.recoverMongosAfterBackup()
        return True

'''
description: like main function
return: None.
''' 
if __name__ == '__main__':
#    backType is 'full' or 'inc'
    backType = sys.argv[1]
    if backType and (str(backType) == 'full' or str(backType) == 'inc'):
        Launcher('mongos_backup.conf', backType) 
    else:
        print 'please input backup type: full or inc!'             
            
            
            
            
            
            
            