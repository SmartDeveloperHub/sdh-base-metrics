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

import pkg_resources

try:
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)

from sdh.metrics.server import app
from sdh.metrics.store.fragment import FragmentStore
import calendar
from datetime import datetime


class SCMStore(FragmentStore):
    def __init__(self):
        super(SCMStore, self).__init__(app.config['REDIS'])

    def get_repositories(self):
        repo_keys = self.db.keys('frag:repos:-*-:')
        repo_data = [{'name': self.db.hget(rk, 'name'), 'uri': rk.lstrip('frag:repos:-').rstrip('-:')}
                     for rk in repo_keys]
        return repo_data

    def get_repository_branches(self, rid):
        branch_set_keys = self.db.keys('frag:repos:-*-:branches')
        repo_data = [{'name': self.db.hget(bsk, 'name'), 'uri': bsk.split(':')[1]} for bsk in branch_set_keys]
        return repo_data

    def get_commits(self, begin=0, end=None, rid=None, bid=None, uid=None, withstamps=False, limit=None, start=None):
        if end is None:
            end = calendar.timegm(datetime.utcnow().timetuple())
        commits = self.db.zrangebyscore('frag:sorted-commits', begin, end, withscores=withstamps,
                                        num=limit, start=start)
        if len(commits):
            if rid is not None:
                r_uri = self.db.get('frag:repos:{}:'.format(rid))
                repo_branches = self.db.smembers('frag:repos:-{}-:branches'.format(r_uri))
                filtered_commits = [self.db.smembers('frag:branches:-{}-:commits'.format(rb)) for rb in repo_branches]
                if filtered_commits:
                    filtered_commits = set.union(*filtered_commits)
                commits = set.intersection(set(commits), filtered_commits)
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
                repo_branches = self.db.smembers('frag:repos:-{}-:branches'.format(rid))
                if len(repo_branches):
                    branches = set.intersection(set(branches), repo_branches)

        return branches

    def get_developers(self, begin=0, end=None, rid=None, withstamps=False, limit=None, start=None):
        commits = self.get_commits(begin, end, rid=rid, withstamps=withstamps, limit=limit, start=start)
        if len(commits):
            developers = set([self.db.hget('frag:commits:-{}-'.format(c), 'by') for c in commits])
            developers = filter(lambda x: x is not None, developers)
            developers = [(d_uri, self.db.hget('frag:devs:-{}-'.format(d_uri), 'id')) for d_uri in developers]
            return developers
        return commits

    @property
    def first_date(self):
        now = calendar.timegm(datetime.utcnow().timetuple())
        _, t_ini = self.get_commits(0, now, withstamps=True, start=0, limit=1).pop()
        return t_ini


store = SCMStore()
