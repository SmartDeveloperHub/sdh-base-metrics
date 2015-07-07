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

from sdh.metrics.server import app
from sdh.metrics.store.scm import store
import itertools


def __aggregate_time_data(key, begin, end, num, step, aggr):
    step_begin = begin
    values = []
    while step_begin <= end - step:
        step_end = step_begin + step
        result = [eval(res)['v'] for res in store.db.zrangebyscore(key, step_begin, step_end)]
        values.append(result)
        step_begin = step_end

    if not num:
        _, t_ini = store.db.zrangebyscore(key, begin, end, withscores=True, start=0, num=1).pop()
        elm_0 = values.pop()
        if any(isinstance(el, list) for el in elm_0):
            elm_0 = [len(x) for x in elm_0]
        return t_ini, elm_0

    return [aggr(part) for part in values]


def __avg_aggr(x):
    if type(x) == list:
        if x:
            return sum(x) / float(len(x))
    return 0


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


def _update_interval_repo_commits(begin, end):
    for repo in store.get_repositories():
        value = len(store.get_commits(begin, end, rid=repo['name']))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-repo-commits:{}'.format(repo['name']), begin, obj_value)


def _update_interval_user_commits(begin, end):
    for _, uid in store.get_developers(begin, end):
        value = len(store.get_commits(begin, end, uid=uid))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-user-commits:{}'.format(uid), begin, obj_value)


def _update_interval_commits(begin, end):
    value = len(store.get_commits(begin, end))
    obj_value = {'t': begin, 'v': value}
    store.update_set('metrics:total-commits', begin, obj_value)


def _update_interval_branches(begin, end):
    value = len(store.get_branches(begin, end))
    obj_value = {'t': begin, 'v': value}
    store.update_set('metrics:total-branches', begin, obj_value)


def _update_interval_repo_branches(begin, end):
    for repo in store.get_repositories():
        value = len(store.get_branches(begin, end, rid=repo['uri']))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-repo-branches:{}'.format(repo['name']), begin, obj_value)


def _update_interval_developers(begin, end):
    value = len(store.get_developers(begin, end))
    obj_value = {'t': begin, 'v': value}
    store.update_set('metrics:total-developers', begin, obj_value)


def _update_interval_repo_developers(begin, end):
    for repo in store.get_repositories():
        value = len(store.get_developers(begin, end, rid=repo['uri']))
        obj_value = {'t': begin, 'v': value}
        store.update_set('metrics:total-repo-developers:{}'.format(repo['name']), begin, obj_value)


@app.repometric('/total-repo-commits', 'total-commits', calculus=_update_interval_repo_commits)
def get_total_repo_commits(rid, **kwargs):
    return __aggregate_time_data('metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], lambda x: sum(x))


@app.orgmetric('/total-commits', 'total-commits', calculus=_update_interval_commits)
def get_total_org_commits(**kwargs):
    return __aggregate_time_data('metrics:total-commits', kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], lambda x: sum(x))


@app.usermetric('/total-user-commits', 'total-commits', calculus=_update_interval_user_commits)
def get_total_user_commits(uid, **kwargs):
    return __aggregate_time_data('metrics:total-user-commits:{}'.format(uid), kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], lambda x: sum(x))


@app.repometric('/avg-repo-commits', 'avg-commits')
def get_avg_repo_commits(rid, **kwargs):
    return __aggregate_time_data('metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'],
                                 __avg_aggr)


@app.orgmetric('/avg-commits', 'avg-commits')
def get_avg_org_commits(**kwargs):
    return __aggregate_time_data('metrics:total-commits', kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], __avg_aggr)


@app.orgmetric('/total-branches', 'total-branches', calculus=_update_interval_branches)
def get_total_org_branches(**kwargs):
    return __aggregate_time_data('metrics:total-branches', kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], lambda x: sum(x))


@app.repometric('/total-repo-branches', 'total-branches', calculus=_update_interval_repo_branches)
def get_total_repo_branches(rid, **kwargs):
    return __aggregate_time_data('metrics:total-repo-branches:{}'.format(rid), kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], lambda x: sum(x))


@app.orgmetric('/avg-branches', 'avg-branches')
def get_avg_org_branches(**kwargs):
    return __aggregate_time_data('metrics:total-branches', kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], __avg_aggr)


@app.orgmetric('/total-developers', 'total-developers', calculus=_update_interval_developers)
def get_total_org_developers(**kwargs):
    def __aggr(x):
        if any(isinstance(el, list) for el in x):
            chain = itertools.chain(*x)
            return len(set(list(chain)))
        else:
            return sum(x)

    return __aggregate_time_data('metrics:total-developers', kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], __aggr)


@app.repometric('/total-repo-developers', 'total-developers', calculus=_update_interval_repo_developers)
def get_total_repo_developers(rid, **kwargs):
    def __aggr(x):
        chain = itertools.chain(*x)
        return len(set(list(chain)))

    return __aggregate_time_data('metrics:total-repo-developers:{}'.format(rid), kwargs['begin'], kwargs['end'],
                                 kwargs['num'], kwargs['step'], __aggr)
