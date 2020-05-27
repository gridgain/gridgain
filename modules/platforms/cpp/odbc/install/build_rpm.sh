#!/bin/bash

WIN_GRIDGAIN_VERSION=8.7.17

IGNITE_ROOT=$1
RPMBUILD_DIR=$2

CPP_ROOT="$IGNITE_ROOT/modules/platforms/cpp"
SPEC_NAME="gridgain-odbc.spec"

mkdir -p "$RPMBUILD_DIR/BUILD"     || exit 1
mkdir -p "$RPMBUILD_DIR/BUILDROOT" || exit 1
mkdir -p "$RPMBUILD_DIR/RPMS"      || exit 1
mkdir -p "$RPMBUILD_DIR/SOURCES"   || exit 1
mkdir -p "$RPMBUILD_DIR/SPECS"     || exit 1
mkdir -p "$RPMBUILD_DIR/SRPMS"     || exit 1
mkdir -p "$RPMBUILD_DIR/TMP"       || exit 1

# Copying specification for building RPM
cp "$CPP_ROOT/odbc/install/$SPEC_NAME" "$RPMBUILD_DIR/SPECS" || exit 1

# Creating tar archive from sources in a needed format for build RPM
ARCHIVE_BUILD_DIR="$RPMBUILD_DIR/TMP/gridgain-$WIN_GRIDGAIN_VERSION"

mkdir -p "$ARCHIVE_BUILD_DIR/modules/platforms" || exit 1
cp "$IGNITE_ROOT/LICENSE" "$ARCHIVE_BUILD_DIR"  || exit 1
cp -r "$IGNITE_ROOT/modules/platforms/cpp" "$ARCHIVE_BUILD_DIR/modules/platforms"  || exit 1

pushd "$ARCHIVE_BUILD_DIR/.."
tar -czf "$RPMBUILD_DIR/SOURCES/$WIN_GRIDGAIN_VERSION.tar.gz" "gridgain-$WIN_GRIDGAIN_VERSION" || exit 1
popd

# Building RPM...
rpmbuild --define "_topdir $RPMBUILD_DIR" -bb "$RPMBUILD_DIR/SPECS/$SPEC_NAME"

