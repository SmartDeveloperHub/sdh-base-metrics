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
from sdh.metrics.store import store

@store.collect('?r scm:hasBranch ?b')
def link_repo_branch((r_uri, _, b_uri)):
    store.execute(lambda: store.pipe.sadd('frag:repos:-{}-:branches'.format(r_uri), b_uri))

@store.collect('?r doap:name ?n')
def add_repository((r_uri, _, name)):
    store.execute(lambda: store.pipe.hset('frag:repos:-{}-:'.format(r_uri), 'name', name.toPython()))
    store.execute(lambda: store.pipe.set('frag:repos:{}:'.format(name.toPython()), r_uri))

@store.collect('?c scm:createdOn ?t')
def add_commit((c_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    store.execute(lambda: store.pipe.zadd('frag:sorted-commits', timestamp, c_uri))

@store.collect('?b scm:createdOn ?tb')
def add_branch((b_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    store.execute(lambda: store.pipe.zadd('frag:sorted-branches', timestamp, b_uri))

@store.collect('?b scm:hasCommit ?c')
def link_branch_commit((b_uri, _, c_uri)):
    store.execute(lambda: store.pipe.sadd('frag:branches:-{}-:commits'.format(b_uri), c_uri))

@store.collect('?c scm:performedBy ?pc')
def link_commit_developer((c_uri, _, d_uri)):
    store.execute(lambda: store.pipe.hset('frag:commits:-{}-'.format(c_uri), 'by', d_uri))
    store.execute(lambda: store.pipe.sadd('frag:devs:-{}-:commits'.format(d_uri), c_uri))

@store.collect('?r doap:developer ?p')
def link_repo_developer((r_uri, _, d_uri)):
    store.execute(lambda: store.pipe.sadd('frag:repos:-{}-:devs'.format(r_uri), d_uri))

@store.collect('?p foaf:name ?pn')
def set_developer_name((d_uri, _, name)):
    store.execute(lambda: store.pipe.hset('frag:devs:-{}-'.format(d_uri), 'name', name.toPython()))

@store.collect('?p scm:userId ?pid')
def set_developer_id((d_uri, _, uid)):
    store.execute(lambda: store.pipe.set('frag:devs:{}:'.format(uid.toPython()), d_uri))
    store.execute(lambda: store.pipe.hset('frag:devs:-{}-'.format(d_uri), 'id', uid.toPython()))
