Name:           gridgain-odbc
Version:        8.7.17
Release:        1%{?dist}
Summary:        ODBC driver for GridGain.

License:        GridGain Community Edition License
URL:            https://www.gridgain.com/
Vendor:         GridGain Systems
Source0:        https://github.com/gridgain/gridgain/archive/%{version}.tar.gz

BuildRequires:  gcc
BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  automake
BuildRequires:  autoconf
BuildRequires:  libtool
BuildRequires:  openssl-devel
BuildRequires:  unixODBC-devel

Requires:       openssl
Requires:       unixODBC

%description
The GridGain Community Edition (GCE) is a source-available in-memory computing platform built on Apache Ignite. 
GCE includes the Apache Ignite code base plus improvements and additional functionality developed by GridGain Systems 
to enhance the performance and reliability of Apache Ignite. GCE is a hardened, high-performance version of Apache Ignite 
for mission-critical in-memory computing applications.

GridGain provides ODBC driver that can be used to retrieve distributed
data from cache using standard SQL queries and native ODBC API.

For information on how to get started with GridGain please visit:

    https://docs.gridgain.com/docs

%prep
%setup -q -n "gridgain-%{version}"
cp LICENSE modules/platforms/cpp

%build
cd modules/platforms/cpp
libtoolize
aclocal
autoheader
automake --add-missing
autoreconf
%configure --disable-core --disable-thin-client --disable-node --disable-tests --enable-odbc
make %{?_smp_mflags}

%install
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
cd modules/platforms/cpp
%make_install
rm -rf $RPM_BUILD_ROOT/%{_includedir}
rm -rf $RPM_BUILD_ROOT/%{_libdir}/*.a
rm -rf $RPM_BUILD_ROOT/%{_libdir}/*.la

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%license LICENSE
%doc modules/platforms/cpp/odbc/README.txt
%{_libdir}/libignite-binary.so
%{_libdir}/libignite-binary-%{version}*
%{_libdir}/libignite-common.so
%{_libdir}/libignite-common-%{version}*
%{_libdir}/libignite-network.so
%{_libdir}/libignite-network-%{version}*
%{_libdir}/libignite-odbc.so
%{_libdir}/libignite-odbc-%{version}*

%post
echo '[Apache Ignite]
Description=Apache Ignite ODBC Driver
Driver=%{_libdir}/libignite-odbc.so
Setup=%{_libdir}/libignite-odbc.so
DriverODBCVer=03.00' \
> %{_tmppath}/%{name}-%{version}-%{release}.ini

odbcinst -i -d -f %{_tmppath}/%{name}-%{version}-%{release}.ini -v

%preun
odbcinst -u -d -n "Apache Ignite" -v

%changelog
* Tue May 19 2020 Igor Sapego <isapego@gridgain.com> - 8.7.17-1
- First version of RPM package for GridGain ODBC driver

