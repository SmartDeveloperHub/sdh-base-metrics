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

from setuptools import setup, find_packages

setup(
    name="SDH-SCM-Metrics",
    version="0.2.16",
    author="Fernando Serena",
    author_email="fernando.serena@centeropenmiddleware.com",
    description="A service that calculates a small set of quantitative metrics on data from a GitLab SCM.",
    license="Apache 2",
    keywords=["linked-data", "ontology", "path", "sdh", "metrics"],
    url="https://github.com/smartdeveloperhub/sdh-scm-metrics",
    download_url="https://github.com/smartdeveloperhub/sdh-scm-metrics/tarball/0.0.2",
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['sdh', 'sdh.metrics'],
    install_requires=['SDH-Metrics'],
    classifiers=[],
    scripts=['sdh-scm-metrics']
)
