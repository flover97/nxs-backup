#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import sysrsync
import tarfile

import config
import log_and_mail
import general_function
import periodic_backup




sysrsync.run(source='/home/user/files',
             destination='/home/server/files',
             destination_ssh='myserver',
             options=['-a'],
             exclusions=['file_to_exclude', 'unwanted_file'])
# runs 'rsync -a /home/users/files/ myserver:/home/server/files --exclude file_to_exclude --exclude unwanted_file'
