#!/bin/bash

sed "s|{@current_directory}|$(pwd)|g" docker-compose.template.yml > generated-docker-compose.yml