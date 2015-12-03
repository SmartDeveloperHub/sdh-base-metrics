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
from sdh.metrics.scm import app, st as store
from sdh.metrics.store.metrics import aggregate, avg
from sdh.metrics.server import SCM, ORG
import itertools

__author__ = 'Fernando Serena'


@app.view('/repositories', target=SCM.Repository)
def get_repositories(**kwargs):
    return store.get_repositories()


@app.view('/branches', target=SCM.Branch)
def get_branches(**kwargs):
    return list(store.get_branches(kwargs['begin'], kwargs['end']))


@app.view('/commits', target=SCM.Commit)
def get_commits(**kwargs):
    return list(store.get_commits(kwargs['begin'], kwargs['end']))


@app.view('/member-commits', target=SCM.Commit, parameters=[ORG.Person])
def get_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id))


@app.view('/member-repositories', target=SCM.Repository, parameters=[ORG.Person])
def get_member_repositories(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    commits = store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id)
    return list(store.get_commits_repos(commits))


@app.view('/member-repo-commits', target=SCM.Commit, parameters=[SCM.Repository, ORG.Person])
def get_member_repo_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id, rid=rid))


@app.view('/developers', target=ORG.Person)
def get_developers(**kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'])
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x), devs))
    return list(devs)


@app.view('/repo-developers', parameters=[SCM.Repository], target=ORG.Person, title='Developers')
def get_repo_developers(rid, **kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'], rid=rid)
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x), devs))
    return list(devs)


@app.metric('/total-repo-commits', parameters=[SCM.Repository], title='Commits')
def get_total_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-commits', title='Commits')
def get_total_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-repositories', title='Repository')
def get_total_org_repositories(**kwargs):
    return {}, [len(store.get_repositories())]


@app.metric('/total-member-commits', parameters=[ORG.Person], title='Commits')
def get_total_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-repo-member-commits', parameters=[SCM.Repository, ORG.Person], title='Commits')
def get_total_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'])


@app.metric('/avg-repo-member-commits', aggr='avg', parameters=[SCM.Repository, ORG.Person], title='Commits')
def get_avg_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/avg-member-commits', aggr='avg', parameters=[ORG.Person], title='Commits')
def get_avg_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/member-longest-streak', parameters=[ORG.Person], title='Longest Streak')
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


@app.metric('/avg-repo-commits', aggr='avg', parameters=[SCM.Repository], title='Commits/repo')
def get_avg_repo_commits(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-commits:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/avg-commits', aggr='avg', title='Commits')
def get_avg_org_commits(**kwargs):
    return aggregate(store, 'metrics:total-commits', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/total-branches', title='Commits')
def get_total_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-repo-branches', parameters=[SCM.Repository], title='Branches')
def get_total_repo_branches(rid, **kwargs):
    return aggregate(store, 'metrics:total-repo-branches:{}'.format(rid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/avg-branches', aggr='avg', title='Branches')
def get_avg_org_branches(**kwargs):
    return aggregate(store, 'metrics:total-branches', kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.metric('/total-developers', title='Developers')
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


@app.metric('/total-repo-developers', parameters=[SCM.Repository], title='Developers')
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


@app.metric('/total-product-commits', parameters=[ORG.Product], title='Commits')
def get_total_product_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-product-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-project-commits', parameters=[ORG.Project], title='Commits')
def get_total_project_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-project-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-product-branches', parameters=[ORG.Product], title='Branches')
def get_total_product_branches(prid, **kwargs):
    return aggregate(store, 'metrics:total-product-branches:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.metric('/total-project-branches', parameters=[ORG.Project], title='Branches')
def get_total_project_branches(prid, **kwargs):
    return aggregate(store, 'metrics:total-project-branches:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])
