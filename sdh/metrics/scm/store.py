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

from sdh.metrics.store.fragment import FragmentStore
import calendar
from datetime import datetime
import uuid
import re


class SCMStore(FragmentStore):
    def __init__(self, **kwargs):
        super(SCMStore, self).__init__(**kwargs)

    def get_repositories(self):
        repo_keys = self.db.keys('frag:repos:-*-:')
        return filter(lambda x: x is not None, [self.db.hget(rk, 'id') for rk in repo_keys])

    def get_products(self):
        repo_keys = self.db.keys('frag:products:-*-:')
        return filter(lambda x: x is not None, [self.db.hget(rk, 'name') for rk in repo_keys])

    def get_repository_branches(self, rid):
        branch_set_keys = self.db.keys('frag:repos:-*-:branches')
        repo_data = [{'name': self.db.hget(bsk, 'name'), 'uri': bsk.split(':')[1]} for bsk in branch_set_keys]
        return repo_data

    def get_commits_repos(self, commits):
        temp_key = str(uuid.uuid4())
        self.db.sadd(temp_key, *commits)
        all_branches_keys = self.db.keys('frag:repos:-*-:branches')
        repos = set([])
        pattern = re.compile(r':-(.+?)-:')
        for b_key in all_branches_keys:
            r_uri = pattern.findall(b_key).pop()
            rid = self.db.hget('frag:repos:-{}-:'.format(r_uri), 'id')
            if rid not in repos:
                repo_branches = self.db.smembers(b_key)
                for b_uri in repo_branches:
                    branch_commits_key = 'frag:branches:-{}-:commits'.format(b_uri)
                    if self.db.sinter(temp_key, branch_commits_key):
                        repos.add(rid)
        self.db.delete(temp_key)
        return repos

    def get_commits(self, begin=0, end=None, rid=None, bid=None, uid=None, withstamps=False, limit=None, start=None):
        if end is None:
            end = calendar.timegm(datetime.utcnow().timetuple())
        commits = self.db.zrangebyscore('frag:sorted-commits', begin, end, withscores=withstamps,
                                        num=limit, start=start)
        if len(commits):
            if rid is not None:
                r_uri = self.db.get('frag:repos:{}:'.format(rid))
                repo_branches = self.db.smembers('frag:repos:-{}-:branches'.format(r_uri))
                temp_key = str(uuid.uuid4())
                self.db.sadd(temp_key, *commits)
                filtered_commits = set([])
                for branch in repo_branches:
                    filtered_commits.update(self.db.sinter(temp_key, 'frag:branches:-{}-:commits'.format(branch)))
                commits = filtered_commits
                self.db.delete(temp_key)
            elif bid is not None:
                # Do the uri trick as in rid alternative
                filtered_commits = self.db.smembers('frag:branches:-{}-:commits'.format(bid))
                if filtered_commits:
                    filtered_commits = set.union(*filtered_commits)
                commits = set.intersection(set(commits), filtered_commits)
            if uid is not None:
                d_uri = self.db.get('frag:devs:{}:'.format(uid))
                filtered_commits = self.db.smembers('frag:devs:-{}-:commits'.format(d_uri))
                commits = set.intersection(set(commits), filtered_commits)

        return commits

    def get_branches(self, begin=0, end=None, rid=None, withstamps=False, limit=None, start=None):
        if end is None:
            end = calendar.timegm(datetime.utcnow().timetuple())
        branches = self.db.zrangebyscore('frag:sorted-branches', begin, end, withscores=withstamps, num=limit,
                                         start=start)
        if len(branches):
            if rid is not None:
                temp_key = str(uuid.uuid4())
                self.db.sadd(temp_key, *branches)
                r_uri = self.db.get('frag:repos:{}:'.format(rid))
                branches = self.db.sinter('frag:repos:-{}-:branches'.format(r_uri), temp_key)
                self.db.delete(temp_key)

        return branches

    def get_developers(self, begin=0, end=None, rid=None, withstamps=False, limit=None, start=None):
        commits = self.get_commits(begin, end, rid=rid, withstamps=withstamps, limit=limit, start=start)
        if len(commits):
            developers = set([self.db.hget('frag:commits:-{}-'.format(c), 'by') for c in commits])
            developers = [self.db.hget('frag:devs:-{}-'.format(d_uri), 'id') for d_uri in developers]
            return filter(lambda x: x is not None, developers)

        return commits

    @property
    def first_date(self):
        now = calendar.timegm(datetime.utcnow().timetuple())
        _, t_ini = self.get_commits(0, now, withstamps=True, start=0, limit=1).pop()
        return t_ini

    def get_product_projects(self, prid):
        product_uri = self.db.get('frag:products:{}:'.format(prid))
        project_uris = self.db.smembers('frag:products:-{}-:projects'.format(product_uri))
        return map(lambda x: self.db.hget('frag:projects:-{}-:'.format(x), 'name'), project_uris)

    def get_project_repositories(self, prid):
        project_uri = self.db.get('frag:projects:{}:'.format(prid))
        repo_names = self.db.smembers('frag:projects:-{}-:repos'.format(project_uri))
        repo_uris = map(lambda x: self.db.get('frag:reponames:{}:'.format(x)), repo_names)
        return map(lambda x: self.db.hget('frag:repos:-{}-:'.format(x), 'id'), repo_uris)

    def get_member_id(self, mid):
        try:
            member_uri = self.db.get('frag:members:{}:'.format(mid))
            member_nick = self.db.hget('frag:members:-{}-:'.format(member_uri), 'nick')
            if member_nick == 'AlejandroVera':
                member_nick = 'avera'
            committer_uri = self.db.get('frag:devs:{}:'.format(member_nick))
            return self.db.hget('frag:devs:-{}-'.format(committer_uri), 'id')
        except:
            return None

    def get_committer_id(self, cid):
        try:
            committer_uri = self.db.get('frag:devs:{}:'.format(cid))
            committer_nick = self.db.hget('frag:devs:-{}-'.format(committer_uri), 'nick')
            if committer_nick == 'avera':
                committer_nick = 'AlejandroVera'
            member_uri = self.db.get('frag:members:{}:'.format(committer_nick))
            return self.db.hget('frag:members:-{}-:'.format(member_uri), 'id')
        except:
            return None
