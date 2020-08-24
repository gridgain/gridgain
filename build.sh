#!/bin/bash

mvn clean package -U -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Pall-java,all-scala,scala,licenses,lgpl,examples,checkstyle
