#!/usr/bin/python
from apscheduler.scheduler import Scheduler
import ConfigParser
import os,sys,os.path
import copy
import logging
import logging.handlers
from subprocess import Popen, PIPE

abspath = os.path.abspath(sys.argv[0])
dirname = os.path.dirname(abspath)
basename = os.path.basename(sys.argv[0])[:-3]
conffile = dirname + "/" + basename + ".conf"
logfile = dirname + "/" + basename + ".log"

######add logging ######
logger = logging.getLogger(basename)
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(logfile, maxBytes = 2000000, backupCount = 5)
formatter = logging.Formatter(fmt = '%(asctime)s -- %(message)s', datefmt = '%m/%d/%Y %I:%M:%S %p' )
handler.setFormatter(formatter)
logger.addHandler(handler)
######add logging ######

######read config file##########
cf = ConfigParser.ConfigParser()
cf.read(conffile)

scope_set = cf.sections()
default = dict(cf.items("Default"))
options = cf.options("Default")
#print default
#print options
scope_param = []
for scope in scope_set:
    if scope == "Default":
        next
    else:
        #print scope
        #print dict(cf.items(scope))
        scope_d = dict(cf.items(scope))
        scope_d['scope_name'] = scope
        
        for key in options:
            if key in scope_d:
               next
            else:
               scope_d[key] = default[key] 

        #print scope_d
        scope_param.append(scope_d)

#print scope_param
new_param = copy.deepcopy(scope_param)

for i in new_param: 
    #print i
    i.pop('scope_name')

#print new_param

for i in range(len(new_param)):
    if new_param[i].has_key('del_flag'):
        next
    for item in new_param[i+1:]:
        if item.has_key('del_flag'):
            next
        if not cmp(new_param[i],item):
            #print item
            item['del_flag'] = 1

def func_filter(x):
    if not x.has_key('del_flag'):
        return x

uniq = filter(func_filter, new_param)
#print uniq

new_scope_param=[]

for i in uniq:
    #print i
    new_i = copy.deepcopy(i)
    new_i['scope_name'] = ''
    for j in copy.deepcopy(scope_param):
        s=j.pop('scope_name')
        #print s
        if i == j:
            new_i['scope_name'] = new_i['scope_name'] + ' ' + s 
            new_i['scope_name'] = new_i['scope_name'].strip()
    new_scope_param.append(new_i)


#for i in new_scope_param:
#    print i

######read config file##########

######taddm discovery function##########
username='administrator'
password='collation'
def discover_func(prof_name, scope_name):
     cmd = ['/opt/IBM/taddm/dist/sdk/bin/api.sh','-u',username,'-p',password,'discover','start','--profile',prof_name] + scope_name
     ret=Popen(cmd,stdout=PIPE,stderr=PIPE)
     #boutput=ret.communicate()[0]
     #output=str(boutput,encoding='utf-8')
     
     logger.info("starting discovery... scope:" + " ".join(scope_name) +  "\t" + "profile:" + prof_name)
     logger.info(" ".join(cmd))
     #logger.info(output)

######taddm discovery function##########

######Start the scheduler###############
sched = Scheduler()
sched.daemonic = False

for scope in new_scope_param:

    #print scope
    scope_name = scope['scope_name'].split(' ')
    y = scope['year']
    m = scope['month']
    d = scope['day']
    w = scope['week']
    dow = scope['day_of_week']
    h = scope['hour']
    mi = scope['minute']
    s = scope['second']
    sd = scope['start_date']
    prof_name = scope['prof_name']

    if not y: y = None
    if not m: m = None
    if not d: d = None
    if not w: w = None
    if not dow: dow = None
    if not h: h = None
    if not mi: mi = None
    if not s: s = None
    if not sd: sd = None

    #print scope_name

    sched.add_cron_job(discover_func, year=y, month=m, day=d, week=w, day_of_week=dow, hour=h, minute=mi, second=s, start_date=sd, args=[prof_name,scope_name])

sched.start()
