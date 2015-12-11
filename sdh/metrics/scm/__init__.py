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
redis_conf = app.config['REDIS']
st = SCMStore(**redis_conf)
app.store = st


@st.query(['?_ms org:member ?member', '?member org:id ?mid', '?member foaf:nick ?nick'])
def query_members(arg):
    st.execute('set', 'frag:members:{}:'.format(arg['mid']), arg['member'])
    st.execute('hset', 'frag:members:-{}-:'.format(arg['member']), 'id', arg['mid'])

    st.execute('hset', 'frag:members:-{}-:'.format(arg['member']), 'nick', arg['nick'])
    st.execute('set', 'frag:members:{}:'.format(arg['nick']), arg['member'])


@st.query(['?_oh org:hasProduct ?prod', '?prod org:id ?prid'])
def query_products(arg):
    product_uri = arg.get('prod')
    product_name = arg.get('prid')
    st.execute('sadd', 'frag:products', product_uri)
    st.execute('set', 'frag:products:{}:'.format(product_name), product_uri)
    st.execute('hset', 'frag:products:-{}-:'.format(product_uri), 'name', product_name)


@st.query(['?_oh org:hasProject ?proj', '?proj org:id ?pjid', '?proj doap:repository ?repo'])
def query_projects(arg):
    project_uri = arg.get('proj')
    project_name = arg.get('pjid')
    st.execute('sadd', 'frag:projects', project_uri)
    st.execute('set', 'frag:projects:{}:'.format(project_name), project_uri)
    st.execute('hset', 'frag:projects:-{}-:'.format(project_uri), 'name', project_name)
    repo_name = urlparse.urlparse(arg.get('repo')).path.split('/').pop(-1)
    st.execute('sadd', 'frag:projects:-{}-:repos'.format(project_uri),
               repo_name)


@st.query(['?prod org:relatesToProject ?proj'])
def add_project(arg):
    st.execute('sadd', 'frag:products:-{}-:projects'.format(arg.get('prod')), arg.get('proj'))
    st.execute('sadd', 'frag:projects:-{}-:products'.format(arg.get('proj')), arg.get('prod'))


@st.collect('?r scm:hasBranch ?b')
def link_repo_branch((r_uri, _, b_uri)):
    st.execute('sadd', 'frag:repos:-{}-:branches'.format(r_uri), b_uri)


@st.collect('?r doap:name ?n')
def add_repository((r_uri, _, name)):
    st.execute('hset', 'frag:repos:-{}-:'.format(r_uri), 'name', name.toPython())
    st.execute('set', 'frag:reponames:{}:'.format(name), r_uri)


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


@st.collect('?p scm:external ?ext')
def set_external_flag((d_uri, _, ext)):
    st.execute('hset', 'frag:devs:-{}-'.format(d_uri), 'external', ext.toPython())


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
    all_devs = st.get_developers(begin, end)
    devs = map(lambda x: x[0], all_devs)
    externals = map(lambda x: x[0], filter(lambda x: x[1], all_devs))
    total_repo_devs = {}
    total_repo_externals = {}
    repo_product = {}
    repo_project = {}
    product_devs = {}
    project_devs = {}
    product_externals = {}
    project_externals = {}
    products = st.get_products()
    projects = set([])
    for product in products:
        prod_projects = st.get_product_projects(product)
        product_devs[product] = set([])
        product_externals[product] = set([])
        for project in prod_projects:
            project_devs[project] = set([])
            project_externals[project] = set([])
            projects.add(project)
            repos = st.get_project_repositories(project)
            for repo in repos:
                repo_product[repo] = product
                repo_project[repo] = project

    if len(externals):
        store_calc(st, 'metrics:total-externals', begin, externals)
    if len(devs):
        store_calc(st, 'metrics:total-developers', begin, devs)
        for uid in devs:
            value = len(st.get_commits(begin, end, uid=uid))
            store_calc(st, 'metrics:total-member-commits:{}'.format(uid), begin, value)
            for rid in st.get_repositories():
                value = len(st.get_commits(begin, end, uid=uid, rid=rid))
                if rid not in total_repo_devs:
                    total_repo_devs[rid] = set([])
                if rid not in total_repo_externals:
                    total_repo_externals[rid] = set([])
                if value:
                    total_repo_devs[rid].add(uid)
                    if uid in externals:
                        total_repo_externals[rid].add(uid)
                        if rid in repo_product:
                            product_externals.get(repo_product[rid]).add(uid)
                        if rid in repo_project:
                            project_externals.get(repo_project[rid]).add(uid)
                    if rid in repo_product:
                        product_devs.get(repo_product[rid]).add(uid)
                    if rid in repo_project:
                        project_devs.get(repo_project[rid]).add(uid)
                store_calc(st, 'metrics:total-repo-member-commits:{}:{}'.format(rid, uid), begin, value)
        [store_calc(st, 'metrics:total-repo-developers:{}'.format(rid), begin, list(total_repo_devs[rid])) for rid in
         filter(lambda x: len(total_repo_devs[x]), total_repo_devs)]
        [store_calc(st, 'metrics:total-repo-externals:{}'.format(rid), begin, list(total_repo_externals[rid])) for rid
         in
         filter(lambda x: len(total_repo_externals[x]), total_repo_externals)]

        [store_calc(st, 'metrics:total-product-developers:{}'.format(x), begin, list(product_devs[x])) for x in
         products]
        [store_calc(st, 'metrics:total-product-externals:{}'.format(x), begin, list(product_externals[x])) for x in
         products]
        [store_calc(st, 'metrics:total-project-developers:{}'.format(x), begin, list(project_devs[x])) for x in
         projects]
        [store_calc(st, 'metrics:total-project-externals:{}'.format(x), begin, list(project_externals[x])) for x in
         projects]
