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
import calendar
from datetime import datetime
from sdh.fragments.server.base import APIError
from sdh.metrics.scm import app, st as store
from sdh.metrics.store.metrics import aggregate, avg, flat_sum
from sdh.metrics.server import SCM, ORG
import itertools

__author__ = 'Fernando Serena'


@app.view('/member-repositories', target=SCM.Repository, parameters=[ORG.Person],
          id='member-repositories')
def get_member_repositories(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    if committer_id is None:
        return []
    commits = store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id)
    return list(store.get_repo_uris(*store.get_commits_repos(commits)))


@app.view('/developers', target=ORG.Person, id='developers')
def get_developers(**kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'])
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x[0]), devs))
    return list(store.get_developer_uris(*devs))


@app.view('/repo-developers', parameters=[SCM.Repository], target=ORG.Person, title='Developers',
          id='repository-developers')
def get_repo_developers(rid, **kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'], rid=rid)
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x[0]), devs))
    return list(store.get_developer_uris(*devs))


@app.view('/project-developers', parameters=[ORG.Project], target=ORG.Person, title='Developers',
          id='project-developers')
def get_project_developers(pjid, **kwargs):
    repos = store.get_project_repositories(pjid)
    devs = set([])
    for rid in repos:
        devs = set.union(devs, store.get_developers(kwargs['begin'], kwargs['end'], rid=rid))
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x[0]), devs))
    return list(store.get_developer_uris(*set(devs)))


@app.view('/product-developers', parameters=[ORG.Product], target=ORG.Person, title='Developers',
          id='product-developers')
def get_product_developers(prid, **kwargs):
    repos = map(lambda x: store.get_project_repositories(x), store.get_product_projects(prid))
    repos = flat_sum(repos)
    devs = set([])
    for rid in repos:
        devs = set.union(devs, store.get_developers(kwargs['begin'], kwargs['end'], rid=rid))
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x[0]), devs))
    return list(store.get_developer_uris(*set(devs)))


@app.metric('/total-repo-commits', parameters=[SCM.Repository], title='Commits', id='repository-commits')
def get_total_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-commits', title='Commits', id='commits')
def get_total_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-repositories', title='Repository', id='repositories')
def get_total_org_repositories(**kwargs):
    return {}, [len(store.get_repositories())]


@app.metric('/total-member-repositories', parameters=[ORG.Person], id='member-repositories')
def get_total_member_repositories(mid, **kwargs):
    if kwargs['begin'] is None:
        kwargs['begin'] = 0
    if kwargs['end'] is None:
        kwargs['end'] = calendar.timegm(datetime.utcnow().timetuple())

    committer_id = store.get_member_id(mid)
    if committer_id is None:
        return []
    commits = store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id)
    return {}, [len(list(store.get_repo_uris(*store.get_commits_repos(commits))))]


@app.metric('/total-member-commits', parameters=[ORG.Person], title='Commits', id='member-commits')
def get_total_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/member-activity', parameters=[ORG.Person], title='Activity', id='member-activity')
def get_member_activity(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    context, member_res = aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'],
                                    kwargs['end'],
                                    kwargs['max'])
    # Align query params with the local context just obtained
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(member_res)
    try:
        _, global_res = aggregate(store, 'metrics:total-commits', kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(member_res, global_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/repo-activity', parameters=[SCM.Repository], title='Activity', id='repository-activity')
def get_repo_activity(rid, **kwargs):
    if kwargs['begin'] is None and kwargs['end'] is None:
        begin, end = store.get_repo_frame(rid)
        kwargs['begin'] = begin
        kwargs['end'] = end
    context, repo_res = aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
    # Align query params with the local context just obtained
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(repo_res)
    try:
        _, global_res = aggregate(store, 'metrics:total-commits', kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(repo_res, global_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/member-repo-activity', parameters=[SCM.Repository, ORG.Person], title='Activity',
            id='repository-member-activity')
def get_member_repo_activity(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    context, repo_res = aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id),
                                  kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(repo_res)
    try:
        _, member_res = aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(repo_res, member_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/repo-member-activity', parameters=[SCM.Repository, ORG.Person], title='Activity',
            id='member-activity-in-repository')
def get_member_activity_in_repository(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    context, member_res = aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id),
                                    kwargs['begin'],
                                    kwargs['end'],
                                    kwargs['max'])
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(member_res)
    try:
        _, repo_res = aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'],
                                kwargs['end'],
                                kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(member_res, repo_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/project-activity', parameters=[ORG.Project], title='Activity', id='project-activity')
def get_project_activity(pjid, **kwargs):
    if kwargs['begin'] is None and kwargs['end'] is None:
        begin, end = store.get_project_frame(pjid)
        kwargs['begin'] = begin
        kwargs['end'] = end

    context, project_res = aggregate(store, 'metrics:total-project-commits:{}'.format(pjid), kwargs['begin'],
                                     kwargs['end'],
                                     kwargs['max'])
    # Align query params with the local context just obtained
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(project_res)
    try:
        _, global_res = aggregate(store, 'metrics:total-commits', kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(project_res, global_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/product-activity', parameters=[ORG.Product], title='Activity', id='product-activity')
def get_product_activity(prid, **kwargs):
    if kwargs['begin'] is None and kwargs['end'] is None:
        begin, end = store.get_product_frame(prid)
        kwargs['begin'] = begin
        kwargs['end'] = end
    context, project_res = aggregate(store, 'metrics:total-product-commits:{}'.format(prid), kwargs['begin'],
                                     kwargs['end'],
                                     kwargs['max'])
    # Align query params with the local context just obtained
    kwargs['begin'] = int(context['begin'])
    kwargs['end'] = int(context['end'])
    kwargs['max'] = len(project_res)
    try:
        _, global_res = aggregate(store, 'metrics:total-commits', kwargs['begin'],
                                  kwargs['end'],
                                  kwargs['max'])
        activity = [float(m) / float(g) if g else 0 for m, g in zip(project_res, global_res)]
        return context, activity
    except (EnvironmentError, AttributeError) as e:
        raise APIError(e.message)


@app.metric('/total-repo-member-commits', parameters=[SCM.Repository, ORG.Person], title='Commits',
            id='repository-member-commits')
def get_total_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'])


@app.metric('/avg-repo-member-commits', aggr='avg', parameters=[SCM.Repository, ORG.Person], title='Commits',
            id='repository-member-commits')
def get_avg_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/avg-member-commits', aggr='avg', parameters=[ORG.Person], title='Commits', id='member-commits')
def get_avg_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/member-longest-streak', parameters=[ORG.Person], title='Longest Streak', id='member-longest-streak')
def get_member_longest_streak(mid, **kwargs):
    begin = kwargs.get('begin')
    end = kwargs.get('end')

    if begin is None:
        begin = 0
    if end is None:
        end = calendar.timegm(datetime.utcnow().timetuple())

    committer_id = store.get_member_id(mid)
    ts_commits = [ts for (_, ts) in
                  store.db.zrangebyscore('metrics:total-member-commits:{}'.format(committer_id), begin, end,
                                         withscores=True)]

    if ts_commits:
        current_ts = ts_commits.pop(0)
        streak = 1
        max_streak = 1
        for ts in ts_commits:
            if abs(ts - current_ts - 86400) < 1:
                streak += 1
                max_streak = max(streak, max_streak)
            else:
                streak = 1
            current_ts = ts
        return {'begin': begin, 'end': end}, [max_streak]
    else:
        return {}, [0]


@app.metric('/avg-repo-commits', aggr='avg', parameters=[SCM.Repository], title='Commits/repo', id='repository-commits')
def get_avg_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/avg-commits', aggr='avg', title='Commits', id='commits')
def get_avg_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/total-branches', title='Commits', id='branches')
def get_total_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-repo-branches', parameters=[SCM.Repository], title='Branches', id='repository-branches')
def get_total_repo_branches(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-branches:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/avg-branches', aggr='avg', title='Branches', id='branches')
def get_avg_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


def aggr_whole(x):
    return [len(elm) for elm in x]


def dev_aggr(x):
    chain = itertools.chain(*list(x))
    return len(set(list(chain)))


@app.metric('/total-developers', title='Developers', id='developers')
def get_total_org_developers(**kwargs):
    aggr = dev_aggr
    if not kwargs['max']:
        aggr = aggr_whole

    context, result = aggregate(store, 'metrics:total-developers', kwargs['begin'], kwargs['end'],
                                kwargs['max'], aggr, fill=[])
    if aggr == aggr_whole:
        result = result.pop()
    return context, result


@app.metric('/total-externals', title='Externals', id='externals')
def get_total_org_externals(**kwargs):
    aggr = dev_aggr
    if not kwargs['max']:
        aggr = aggr_whole

    context, result = aggregate(store, 'metrics:total-externals', kwargs['begin'], kwargs['end'],
                                kwargs['max'], aggr, fill=[])
    if aggr == aggr_whole:
        result = result.pop()
    return context, result


@app.metric('/total-repo-developers', parameters=[SCM.Repository], title='Developers', id='repository-developers')
def get_total_repo_developers(rid, **kwargs):
    aggr = dev_aggr
    return aggregate(store, 'metrics:total-repo-developers:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr, fill=[])


@app.metric('/total-product-commits', parameters=[ORG.Product], title='Commits', id='product-commits')
def get_total_product_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-product-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-project-commits', parameters=[ORG.Project], title='Commits', id='project-commits')
def get_total_project_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-project-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-product-developers', parameters=[ORG.Product], title='Commits', id='product-developers')
def get_total_product_developers(prid, **kwargs):
    context, result = aggregate(store, 'metrics:total-product-developers:{}'.format(prid), kwargs['begin'],
                                kwargs['end'],
                                kwargs['max'], dev_aggr, fill=[])
    return context, result


@app.metric('/total-project-developers', parameters=[ORG.Project], title='Commits', id='project-developers')
def get_total_project_developers(prid, **kwargs):
    context, result = aggregate(store, 'metrics:total-project-developers:{}'.format(prid), kwargs['begin'],
                                kwargs['end'],
                                kwargs['max'], dev_aggr, fill=[])
    return context, result


@app.metric('/total-product-externals', parameters=[ORG.Product], title='Commits', id='product-externals')
def get_total_product_externals(prid, **kwargs):
    context, result = aggregate(store, 'metrics:total-product-externals:{}'.format(prid), kwargs['begin'],
                                kwargs['end'],
                                kwargs['max'], dev_aggr, fill=[])
    return context, result


@app.metric('/total-project-externals', parameters=[ORG.Project], title='Commits', id='project-externals')
def get_total_project_externals(prid, **kwargs):
    context, result = aggregate(store, 'metrics:total-project-externals:{}'.format(prid), kwargs['begin'],
                                kwargs['end'],
                                kwargs['max'], dev_aggr, fill=[])
    return context, result
