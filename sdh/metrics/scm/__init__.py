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
from sdh.metrics.scm.store import SCMStore
from sdh.metrics.store import store_calc
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


@store.collect('?r scm:repositoryId ?rid')
def add_repository_id((r_uri, _, rid)):
    store.execute('hset', 'frag:repos:-{}-:'.format(r_uri), 'id', rid.toPython())
    store.execute('set', 'frag:repos:{}:'.format(rid.toPython()), r_uri)


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
    for rid in store.get_repositories():
        value = len(store.get_commits(begin, end, rid=rid))
        store_calc(store, 'metrics:total-repo-commits:{}'.format(rid), begin, value)

        value = len(store.get_branches(begin, end, rid=rid))
        store_calc(store, 'metrics:total-repo-branches:{}'.format(rid), begin, value)


@app.calculus(triggers=['add_commit'])
def update_interval_commits(begin, end):
    value = len(store.get_commits(begin, end))
    store_calc(store, 'metrics:total-commits', begin, value)


@app.calculus(triggers=['add_branch'])
def update_interval_branches(begin, end):
    value = len(store.get_branches(begin, end))
    store_calc(store, 'metrics:total-branches', begin, value)


@app.calculus(triggers=['add_commit'])
def update_interval_developers(begin, end):
    devs = store.get_developers(begin, end)
    if len(devs):
        store_calc(store, 'metrics:total-developers', begin, devs)
        total_repo_devs = {}
        for uid in devs:
            value = len(store.get_commits(begin, end, uid=uid))
            store_calc(store, 'metrics:total-user-commits:{}'.format(uid), begin, value)
            for rid in store.get_repositories():
                value = len(store.get_commits(begin, end, uid=uid, rid=rid))
                if rid not in total_repo_devs:
                    total_repo_devs[rid] = set([])
                if value:
                    total_repo_devs[rid].add(uid)
                store_calc(store, 'metrics:total-repo-user-commits:{}:{}'.format(rid, uid), begin, value)
        [store_calc(store, 'metrics:total-repo-developers:{}'.format(rid), begin, list(total_repo_devs[rid])) for rid in
         filter(lambda x: len(total_repo_devs[x]), total_repo_devs)]
