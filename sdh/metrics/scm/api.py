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

from sdh.metrics.scm import app, st as store
from sdh.metrics.store.metrics import aggregate, avg
import itertools


@app.orgtbd('/repositories', 'repositories')
def get_repositories(**kwargs):
    return store.get_repositories()


@app.orgtbd('/branches', 'branches')
def get_branches(**kwargs):
    return list(store.get_branches(kwargs['begin'], kwargs['end']))


@app.orgtbd('/commits', 'commits')
def get_commits(**kwargs):
    return list(store.get_commits(kwargs['begin'], kwargs['end']))


@app.usertbd('/user-commits', 'commits')
def get_user_commits(uid, **kwargs):
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=uid))


@app.usertbd('/user-repositories', 'repositories')
def get_user_repositories(uid, **kwargs):
    commits = store.get_commits(kwargs['begin'], kwargs['end'], uid=uid)
    return list(store.get_commits_repos(commits))


@app.userrepotbd('/user-repo-commits', 'commits')
def get_user_repo_commits(rid, uid, **kwargs):
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=uid, rid=rid))


@app.orgtbd('/developers', 'developers')
def get_developers(**kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'])
    return list(devs)


@app.repotbd('/repo-developers', 'developers')
def get_repo_developers(rid, **kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'], rid=rid)
    return list(devs)


@app.repometric('/total-repo-commits', 'sum', 'commits')
def get_total_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.orgmetric('/total-commits', 'sum', 'commits')
def get_total_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.orgmetric('/total-repositories', 'sum', 'repositories')
def get_total_org_repositories(**kwargs):
    return {}, [len(store.get_repositories())]


@app.usermetric('/total-user-commits', 'sum', 'commits')
def get_total_user_commits(uid, **kwargs):
    return aggregate(store, 'metrics:total-user-commits:{}'.format(uid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.repousermetric('/total-repo-user-commits', 'sum', 'commits')
def get_total_repo_user_commits(rid, uid, **kwargs):
    return aggregate(store, 'metrics:total-repo-user-commits:{}:{}'.format(rid, uid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.repousermetric('/avg-repo-user-commits', 'avg', 'commits')
def get_avg_repo_user_commits(rid, uid, **kwargs):
    return aggregate(store, 'metrics:total-repo-user-commits:{}:{}'.format(rid, uid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg)


@app.usermetric('/avg-user-commits', 'avg', 'commits')
def get_avg_user_commits(uid, **kwargs):
    return aggregate(store, 'metrics:total-user-commits:{}'.format(uid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg)


@app.repometric('/avg-repo-commits', 'avg', 'commits')
def get_avg_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.orgmetric('/avg-commits', 'avg', 'commits')
def get_avg_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.orgmetric('/total-branches', 'sum', 'branches')
def get_total_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.repometric('/total-repo-branches', 'sum', 'branches')
def get_total_repo_branches(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-branches:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.orgmetric('/avg-branches', 'avg', 'branches')
def get_avg_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg)


@app.orgmetric('/total-developers', 'sum', 'developers')
def get_total_org_developers(**kwargs):
    def aggr_whole(x):
        return [len(elm) for elm in x]

    def __aggr(x):
        chain = itertools.chain(*list(x))
        return len(set(list(chain)))

    aggr = __aggr
    if not kwargs['max']:
        aggr = aggr_whole

    context, result = aggregate(store, 'metrics:total-developers', kwargs['begin'], kwargs['end'],
                                kwargs['max'], aggr, fill=[])
    if aggr == aggr_whole:
        result = result.pop()
    return context, result


@app.repometric('/total-repo-developers', 'sum', 'developers')
def get_total_repo_developers(rid, **kwargs):
    def aggr_whole(x):
        return x

    def __aggr(x):
        chain = itertools.chain(*x)
        return len(set(list(chain)))

    aggr = __aggr
    if not kwargs['max']:
        aggr = aggr_whole

    return aggregate(store, 'metrics:total-repo-developers:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr, fill=[])
