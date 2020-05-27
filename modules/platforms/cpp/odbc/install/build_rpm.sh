#!/bin/bash

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

