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
import urlparse

__author__ = 'Fernando Serena'

import calendar
from sdh.metrics.server import MetricsApp
from sdh.metrics.scm.store import SCMStore
from sdh.metrics.store.metrics import store_calc
import os

config = os.environ.get('CONFIG', 'sdh.metrics.scm.config.DevelopmentConfig')

app = MetricsApp(__name__, config)
st = SCMStore(app.config['REDIS'])
app.store = st


@st.collect('?ms org:member ?member')
def search_member(_):
    pass


@st.collect('?member org:id ?mid')
def set_member_id((m_uri, _, mid)):
    st.execute('set', 'frag:members:{}:'.format(mid.toPython()), m_uri)
    st.execute('hset', 'frag:members:-{}-:'.format(m_uri), 'id', mid.toPython())


@st.collect('?member foaf:nick ?m_nick')
def set_member_id((m_uri, _, nick)):
    st.execute('hset', 'frag:members:-{}-:'.format(m_uri), 'nick', nick.toPython())
    st.execute('set', 'frag:members:{}:'.format(nick.toPython()), m_uri)


@st.collect('?oh org:hasProduct ?prod')
def add_product((oh, _, p_uri)):
    st.execute('sadd', 'frag:products', p_uri)


@st.collect('?prod org:id ?prod_id')
def add_product_id((p_uri, _, pid)):
    st.execute('set', 'frag:products:{}:'.format(pid.toPython()), p_uri)
    st.execute('hset', 'frag:products:-{}-:'.format(p_uri), 'name', pid.toPython())


@st.collect('?oh org:hasProject ?proj')
def add_project((oh, _, p_uri)):
    st.execute('sadd', 'frag:projects', p_uri)


@st.collect('?proj org:id ?proj_id')
def add_project_id((p_uri, _, pid)):
    st.execute('set', 'frag:projects:{}:'.format(pid.toPython()), p_uri)
    st.execute('hset', 'frag:projects:-{}-:'.format(p_uri), 'name', pid.toPython())


@st.collect('?proj doap:repository ?repo')
def link_project_repo((pr_uri, _, r_uri)):
    repo_name = urlparse.urlparse(r_uri).path.split('/').pop(-1)
    st.execute('sadd', 'frag:projects:-{}-:repos'.format(pr_uri),
               repo_name)


@st.collect('?prod org:relatesToProject ?lproj')
def add_project((prod_uri, _, proj_uri)):
    st.execute('sadd', 'frag:products:-{}-:projects'.format(prod_uri), proj_uri)
    st.execute('sadd', 'frag:projects:-{}-:products'.format(proj_uri), prod_uri)


@st.collect('?r scm:hasBranch ?b')
def link_repo_branch((r_uri, _, b_uri)):
    st.execute('sadd', 'frag:repos:-{}-:branches'.format(r_uri), b_uri)


@st.collect('?r doap:name ?n')
def add_repository((r_uri, _, name)):
    st.execute('hset', 'frag:repos:-{}-:'.format(r_uri), 'name', name.toPython())


@st.collect('?r scm:repositoryId ?rid')
def add_repository_id((r_uri, _, rid)):
    st.execute('hset', 'frag:repos:-{}-:'.format(r_uri), 'id', rid.toPython())
    st.execute('set', 'frag:repos:{}:'.format(rid.toPython()), r_uri)


@st.collect('?c scm:createdOn ?t')
def add_commit((c_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    st.execute('zadd', 'frag:sorted-commits', timestamp, c_uri)


@st.collect('?b scm:createdOn ?tb')
def add_branch((b_uri, _, created_on)):
    timestamp = calendar.timegm(created_on.toPython().timetuple())
    st.execute('zadd', 'frag:sorted-branches', timestamp, b_uri)


@st.collect('?b scm:hasCommit ?c')
def link_branch_commit((b_uri, _, c_uri)):
    st.execute('sadd', 'frag:branches:-{}-:commits'.format(b_uri), c_uri)


@st.collect('?c scm:performedBy ?pc')
def link_commit_developer((c_uri, _, d_uri)):
    st.execute('hset', 'frag:commits:-{}-'.format(c_uri), 'by', d_uri)
    st.execute('sadd', 'frag:devs:-{}-:commits'.format(d_uri), c_uri)


@st.collect('?r doap:developer ?p')
def link_repo_developer((r_uri, _, d_uri)):
    st.execute('sadd', 'frag:repos:-{}-:devs'.format(r_uri), d_uri)


@st.collect('?p foaf:nick ?com_nick')
def set_developer_name((d_uri, _, nick)):
    st.execute('hset', 'frag:devs:-{}-'.format(d_uri), 'nick', nick.toPython())
    st.execute('set', 'frag:devs:{}:'.format(nick.toPython()), d_uri)


@st.collect('?p scm:committerId ?pid')
def set_developer_id((d_uri, _, uid)):
    st.execute('set', 'frag:devs:{}:'.format(uid.toPython()), d_uri)
    st.execute('hset', 'frag:devs:-{}-'.format(d_uri), 'id', uid.toPython())


@app.calculus(triggers=['add_commit', 'add_branch'])
def update_interval_repo_metrics(begin, end):
    total_commits = {}
    total_branches = {}
    for rid in st.get_repositories():
        value = len(st.get_commits(begin, end, rid=rid))
        total_commits[rid] = value
        store_calc(st, 'metrics:total-repo-commits:{}'.format(rid), begin, value)

        value = len(st.get_branches(begin, end, rid=rid))
        store_calc(st, 'metrics:total-repo-branches:{}'.format(rid), begin, value)
        total_branches[rid] = value

    for product in st.get_products():
        product_commits = 0
        product_branches = 0
        projects = st.get_product_projects(product)
        for project in projects:
            project_commits = 0
            project_branches = 0
            for rid in set.intersection(set(total_commits.keys()), st.get_project_repositories(project)):
                project_commits += total_commits[rid]
                project_branches += total_branches[rid]
            store_calc(st, 'metrics:total-project-commits:{}'.format(project), begin, project_commits)
            store_calc(st, 'metrics:total-project-branches:{}'.format(project), begin, project_branches)
            product_commits += project_commits
            product_branches += project_branches

        store_calc(st, 'metrics:total-product-commits:{}'.format(product), begin, product_commits)
        store_calc(st, 'metrics:total-product-branches:{}'.format(product), begin, product_branches)


@app.calculus(triggers=['add_commit'])
def update_interval_commits(begin, end):
    value = len(st.get_commits(begin, end))
    store_calc(st, 'metrics:total-commits', begin, value)


@app.calculus(triggers=['add_branch'])
def update_interval_branches(begin, end):
    value = len(st.get_branches(begin, end))
    store_calc(st, 'metrics:total-branches', begin, value)


@app.calculus(triggers=['add_commit'])
def update_interval_developers(begin, end):
    devs = st.get_developers(begin, end)
    if len(devs):
        store_calc(st, 'metrics:total-developers', begin, devs)
        total_repo_devs = {}
        for uid in devs:
            value = len(st.get_commits(begin, end, uid=uid))
            store_calc(st, 'metrics:total-member-commits:{}'.format(uid), begin, value)
            for rid in st.get_repositories():
                value = len(st.get_commits(begin, end, uid=uid, rid=rid))
                if rid not in total_repo_devs:
                    total_repo_devs[rid] = set([])
                if value:
                    total_repo_devs[rid].add(uid)
                store_calc(st, 'metrics:total-repo-member-commits:{}:{}'.format(rid, uid), begin, value)
        [store_calc(st, 'metrics:total-repo-developers:{}'.format(rid), begin, list(total_repo_devs[rid])) for rid in
         filter(lambda x: len(total_repo_devs[x]), total_repo_devs)]
