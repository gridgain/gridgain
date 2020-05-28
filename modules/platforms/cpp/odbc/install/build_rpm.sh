#!/bin/bash
#
# Copyright 2020 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


IGNITE_ROOT=$1
RPMBUILD_DIR=$2

CPP_ROOT="$IGNITE_ROOT/modules/platforms/cpp"
SPEC_NAME="gridgain-odbc.spec"

mkdir -p "$RPMBUILD_DIR/BUILD"
mkdir -p "$RPMBUILD_DIR/BUILDROOT"
mkdir -p "$RPMBUILD_DIR/RPMS"
mkdir -p "$RPMBUILD_DIR/SOURCES"
mkdir -p "$RPMBUILD_DIR/SPECS"
mkdir -p "$RPMBUILD_DIR/SRPMS"
mkdir -p "$RPMBUILD_DIR/TMP"

# Copying specification for building RPM
cp "$CPP_ROOT/odbc/install/$SPEC_NAME" "$RPMBUILD_DIR/SPECS"

# Creating tar archive from sources in a needed format for build RPM
ARCHIVE_BUILD_DIR="$RPMBUILD_DIR/TMP/gridgain-$WIN_GRIDGAIN_VERSION"

mkdir -p "$ARCHIVE_BUILD_DIR/modules/platforms"
cp "$IGNITE_ROOT/LICENSE" "$ARCHIVE_BUILD_DIR"
cp -r "$IGNITE_ROOT/modules/platforms/cpp" "$ARCHIVE_BUILD_DIR/modules/platforms"

pushd "$ARCHIVE_BUILD_DIR/.."
tar -czf "$RPMBUILD_DIR/SOURCES/$WIN_GRIDGAIN_VERSION.tar.gz" "gridgain-$WIN_GRIDGAIN_VERSION"
popd

# Building RPM...
rpmbuild --define "_topdir $RPMBUILD_DIR" -bb "$RPMBUILD_DIR/SPECS/$SPEC_NAME"

