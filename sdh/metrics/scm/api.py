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
import itertools

__author__ = 'Fernando Serena'


@app.orgview('/repositories', 'repositories')
def get_repositories(**kwargs):
    return store.get_repositories()


@app.orgview('/branches', 'branches')
def get_branches(**kwargs):
    return list(store.get_branches(kwargs['begin'], kwargs['end']))


@app.orgview('/commits', 'commits')
def get_commits(**kwargs):
    return list(store.get_commits(kwargs['begin'], kwargs['end']))


@app.memberview('/member-commits', 'commits')
def get_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id))


@app.memberview('/member-repositories', 'repositories')
def get_member_repositories(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    commits = store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id)
    return list(store.get_commits_repos(commits))


@app.memberrepoview('/member-repo-commits', 'commits')
def get_member_repo_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return list(store.get_commits(kwargs['begin'], kwargs['end'], uid=committer_id, rid=rid))


@app.orgview('/developers', 'developers')
def get_developers(**kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'])
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x), devs))
    return list(devs)


@app.repoview('/repo-developers', 'developers')
def get_repo_developers(rid, **kwargs):
    devs = store.get_developers(kwargs['begin'], kwargs['end'], rid=rid)
    devs = filter(lambda x: x is not None, map(lambda x: store.get_committer_id(x), devs))
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


@app.membermetric('/total-member-commits', 'sum', 'commits')
def get_total_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.repomembermetric('/total-repo-member-commits', 'sum', 'commits')
def get_total_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'])


@app.repomembermetric('/avg-repo-member-commits', 'avg', 'commits')
def get_avg_repo_member_commits(rid, mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-repo-member-commits:{}:{}'.format(rid, committer_id), kwargs['begin'],
                     kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.membermetric('/avg-member-commits', 'avg', 'commits')
def get_avg_member_commits(mid, **kwargs):
    committer_id = store.get_member_id(mid)
    return aggregate(store, 'metrics:total-member-commits:{}'.format(committer_id), kwargs['begin'], kwargs['end'],
                     kwargs['max'], aggr=avg, extend=True)


@app.membermetric('/member-longest-streak', 'sum', 'streak', title='Longest Streak')
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


@app.repometric('/avg-repo-commits', 'avg', 'commits', title='Commits/repo')
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
                     kwargs['max'], aggr=avg, extend=True)


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


@app.productmetric('/total-product-commits', 'sum', 'commits')
def get_total_product_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-product-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.projectmetric('/total-project-commits', 'sum', 'commits')
def get_total_project_commits(prid, **kwargs):
    return aggregate(store, 'metrics:total-project-commits:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.productmetric('/total-product-branches', 'sum', 'branches')
def get_total_product_branches(prid, **kwargs):
    return aggregate(store, 'metrics:total-product-branches:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])


@app.projectmetric('/total-project-branches', 'sum', 'branches')
def get_total_project_branches(prid, **kwargs):
    return aggregate(store, 'metrics:total-project-branches:{}'.format(prid), kwargs['begin'], kwargs['end'],
                     kwargs['max'])
