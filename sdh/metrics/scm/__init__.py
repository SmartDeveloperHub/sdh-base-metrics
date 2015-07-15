"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at 

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

__author__ = 'Fernando Serena'

import calendar
from sdh.metrics.server import MetricsApp
from sdh.metrics.store.scm import SCMStore
from datetime import datetime, timedelta
import types
import os

config = os.environ.get('CONFIG', 'sdh.metrics.scm.config.DevelopmentConfig')

app = MetricsApp(__name__, config)
store = SCMStore(app.config['REDIS'])
app.store = store


@store.collect('?r scm:hasBranch ?b')
def link_repo_branch((r_uri, _, b_uri)):
    store.execute('sadd', 'frag:repos:-{}-:branches'.format(r_uri), b_uri)


@store.collect('?r doap:name ?n')
def add_repository((r_uri, _, name)):
    store.execute('hset', 'frag:repos:-{}-:'.format(r_uri), 'name', name.toPython())
    store.execute('set', 'frag:repos:{}:'.format(name.toPython()), r_uri)


@store.collect('?c scm:createdOn ?t')
def add_commit((c_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    store.execute('zadd', 'frag:sorted-commits', timestamp, c_uri)


@store.collect('?b scm:createdOn ?tb')
def add_branch((b_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    store.execute('zadd', 'frag:sorted-branches', timestamp, b_uri)


@store.collect('?b scm:hasCommit ?c')
def link_branch_commit((b_uri, _, c_uri)):
    store.execute('sadd', 'frag:branches:-{}-:commits'.format(b_uri), c_uri)


@store.collect('?c scm:performedBy ?pc')
def link_commit_developer((c_uri, _, d_uri)):
    store.execute('hset', 'frag:commits:-{}-'.format(c_uri), 'by', d_uri)
    store.execute('sadd', 'frag:devs:-{}-:commits'.format(d_uri), c_uri)


@store.collect('?r doap:developer ?p')
def link_repo_developer((r_uri, _, d_uri)):
    store.execute('sadd', 'frag:repos:-{}-:devs'.format(r_uri), d_uri)


@store.collect('?p foaf:name ?pn')
def set_developer_name((d_uri, _, name)):
    store.execute('hset', 'frag:devs:-{}-'.format(d_uri), 'name', name.toPython())


@store.collect('?p scm:userId ?pid')
def set_developer_id((d_uri, _, uid)):
    store.execute('set', 'frag:devs:{}:'.format(uid.toPython()), d_uri)
    store.execute('hset', 'frag:devs:-{}-'.format(d_uri), 'id', uid.toPython())


@app.calculus(triggers=['add_commit', 'add_branch'])
def update_interval_repo_metrics(begin, end):
    for repo in store.get_repositories():
        value = len(store.get_commits(begin, end, rid=repo['name']))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-repo-commits:{}'.format(repo['name']), begin, obj_value)

        value = len(store.get_branches(begin, end, rid=repo['uri']))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-repo-branches:{}'.format(repo['name']), begin, obj_value)


def update_interval_user_commits(begin, end):
    pass
    # for _, uid in store.get_developers(begin, end):
    #     value = len(store.get_commits(begin, end, uid=uid))
    #     obj_value = {'t': begin, 'v': value}
    #     store.update_set('metrics:total-user-commits:{}'.format(uid), begin, obj_value)


@app.calculus(triggers=['add_commit'])
def update_interval_commits(begin, end):
    value = len(store.get_commits(begin, end))
    obj_value = {'t': begin, 'v': value}
    store.update_set('metrics:total-commits', begin, obj_value)


@app.calculus(triggers=['add_branch'])
def update_interval_branches(begin, end):
    value = len(store.get_branches(begin, end))
    obj_value = {'t': begin, 'v': value}
    store.update_set('metrics:total-branches', begin, obj_value)


def update_interval_developers(begin, end):
    pass
    # value = len(store.get_developers(begin, end))
    # obj_value = {'t': begin, 'v': value}
    # store.update_set('metrics:total-developers', begin, obj_value)


def update_interval_repo_developers(begin, end):
    pass
    # for repo in store.get_repositories():
    #     value = len(store.get_developers(begin, end, rid=repo['uri']))
    #     obj_value = {'t': begin, 'v': value}
    #     store.update_set('metrics:total-repo-developers:{}'.format(repo['name']), begin, obj_value)


def aggregate(key, begin, end, num, step, aggr=sum):
    step_begin = begin
    values = []

    def build_time_chunk(ts_dict, begin, end):
        _next = begin
        while _next <= end:
            if _next in ts_dict.keys():
                yield ts_dict[_next]
            else:
                yield 0
            _next = calendar.timegm((datetime.utcfromtimestamp(_next) + timedelta(days=1)).timetuple())

    while step_begin <= end - step:
        step_end = step_begin + step
        if aggr == sum:
            chunk = [eval(res)['v'] for res in store.db.zrangebyscore(key, step_begin, step_end)]
        else:
            stored_values = dict([(int(ts), eval(res)['v']) for res, ts in
                                  store.db.zrangebyscore(key, step_begin, step_end, withscores=True)])
            chunk = build_time_chunk(stored_values, step_begin, step_end)
        values.append(chunk)
        step_begin = step_end

    if not num:
        _, t_ini = store.db.zrangebyscore(key, begin, end, withscores=True, start=0, num=1).pop()
        elm_0 = values.pop()
        if any(isinstance(el, list) for el in elm_0):
            elm_0 = [len(x) for x in elm_0]
        return t_ini, elm_0

    return [aggr(part) for part in values]


def avg(x):
    if isinstance(x, types.GeneratorType):
        x = list(x)
    if type(x) == list:
        if x:
            return sum(x) / float(len(x))
    return 0
