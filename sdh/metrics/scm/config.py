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

import logging

class Config(object):
    STORE_PATHS = {
        'graph': 'graph_store'
    }
    PORT = 5003


class DevelopmentConfig(Config):
    DEBUG = True
    LOG = logging.DEBUG
    PROVIDER = {
        'broker_host': 'localhost',
        'agora_host': 'http://localhost',
        'agora_port': 9002
    }
    REDIS = {
        'host': 'localhost',
        'db': 6
    }


class ProductionConfig(Config):
    DEBUG = False
    LOG = logging.INFO
    REDIS = {
        'host': 'redis',
        'db': 6
    }
    PROVIDER = {
        'broker_host': '138.4.249.224',
        'agora_host': 'http://138.4.249.224',
        'agora_port': 9009
    }
