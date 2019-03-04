#!/bin/bash

if [ -e "${MVN_CENTRAL_GPG_KEY_SEC}" ]
then
  gpg -q --allow-secret-key-import --import ${MVN_CENTRAL_GPG_KEY_SEC} || echo 'Private GPG Sign Key is already imported!.'
  rm ${MVN_CENTRAL_GPG_KEY_SEC}
else
  echo 'Private GPG Key not found.'
fi

if [ -e "${MVN_CENTRAL_GPG_KEY_PUB}" ]
then
  gpg -q --import ${MVN_CENTRAL_GPG_KEY_PUB} || echo 'Public GPG Sign Key is already imported!.'
  rm ${MVN_CENTRAL_GPG_KEY_PUB}
else
  echo 'Public GPG Key not found.'
fi
