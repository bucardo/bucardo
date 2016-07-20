%define         realname Bucardo
%define         sysuser postgres
Name:           bucardo
Version:        5.1.2
Release:        1%{?dist}
Summary:        Postgres replication system for both multi-master and multi-slave operations

Group:          Applications/Databases
License:        BSD
URL:            http://bucardo.org/
Source0:        http://bucardo.org/downloads/Bucardo-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Source1: master-master-replication-example.txt
Source2: bucardo.init
Patch0:  bucardo-logfiles.patch

BuildArch:     noarch

BuildRequires:  perl(ExtUtils::MakeMaker)
BuildRequires:  perl(DBI)
BuildRequires:  perl(DBD::Pg)
BuildRequires:  perl(IO::Handle)
BuildRequires:  perl(Sys::Hostname)
BuildRequires:  perl(Sys::Syslog)
BuildRequires:  perl(Net::SMTP)
BuildRequires:  perl(List::Util)
BuildRequires:  perl(Pod::Usage)
# available from fedora 10
BuildRequires:  perl(DBIx::Safe)
BuildRequires:  perl(boolean)

Requires:  postgresql
Requires:  postgresql-plperl
Requires:  perl(ExtUtils::MakeMaker)
Requires:  perl(DBI)
Requires:  perl(DBD::Pg)
Requires:  perl(DBIx::Safe)
Requires:  perl(IO::Handle)
Requires:  perl(Sys::Hostname)
Requires:  perl(Sys::Syslog)
Requires:  perl(Net::SMTP)
Requires:  perl(List::Util)
Requires:  perl(Pod::Usage)
Requires:  perl(boolean)

#testsuite
Requires:  perl(Test::Simple)
Requires:  perl(Test::Harness)

%description
Bucardo is an asynchronous PostgreSQL replication system, allowing for both
multi-master and multi-slave operations.It was developed at Backcountry.com
primarily by Greg Sabino Mullane of End Point Corporation.

%pre
mkdir -p /var/run/bucardo
mkdir -p /var/log/bucardo
chown -R %{sysuser}:%{sysuser} /var/run/bucardo
chown -R %{sysuser}:%{sysuser} /var/log/bucardo

%post
/sbin/chkconfig --add bucardo

%preun
if [ $1 = 0 ]; then
	/sbin/service bucardo stop > /dev/null 2>&1
	/sbin/chkconfig --del bucardo
fi

%prep
%setup -q -n %{realname}-%{version}
%patch0 -p0

%build

%{__perl} Makefile.PL INSTALLDIRS=vendor

make %{?_smp_mflags}

%install
rm -rf %{buildroot}

make pure_install PERL_INSTALL_ROOT=%{buildroot}

find %{buildroot} -type f -name .packlist -exec rm -f {} +
find %{buildroot} -depth -type d -exec rmdir {} 2>/dev/null \;

sed -i -e '1d;2i#!/usr/bin/perl' bucardo

rm -f %{buildroot}/%{_bindir}/bucardo
install -Dp -m 755 bucardo %{buildroot}/%{_sbindir}/bucardo

install -Dp -m 644 %{SOURCE1} .

# install init script
install -d %{buildroot}/etc/rc.d/init.d
install -m 755 %{SOURCE2} %{buildroot}/etc/rc.d/init.d/%{name}

%{_fixperms} %{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc bucardo.html Bucardo.pm.html Changes
%doc INSTALL LICENSE README SIGNATURE TODO
%doc master-master-replication-example.txt
%{perl_vendorlib}/*
%{_mandir}/man1/*
%{_mandir}/man3/*
%{_sbindir}/bucardo
%{_datadir}/bucardo/bucardo.schema
%{_initrddir}/%{name}

%changelog
* Thu Oct 23 2014 Ivan Poddubny <ivan.poddubny@gmail.com> - 5.1.2-1
- Update to 5.1.2
- Register bucardo as a service with chkconfig
- Require postgresql-plperl

* Wed Nov 7 2012 David E. Wheeler <david@justatheory.com> - 4.99.6-2
- Changed LOGDEST to support multiple destinations.
- Added a two-second sleep between stop and start in the init script restart
  command.

* Tue Nov 6 2012 David E. Wheeler <david@justatheory.com> - 4.99.6-1
- Update to 4.99.6.

* Wed Oct 24 2012 David E. Wheeler <david@justatheory.com> - 4.99.5-3
- Fixed the init script so that the `stop` command actually works.
- Added a patch to point all of the log files to the log directory.

* Tue Oct 2 2012 David E. Wheeler <david@justatheory.com> - 4.99.5-2
- Require postgres.
- Add start script running as postgres.
- Add log directory.

* Fri Sep 28 2012 David E. Wheeler <david@justatheory.com> - 4.99.5-1
- Update to 4.99.5

* Thu Sep 6 2012 Devrim GÜNDÜZ <devrim@gunduz.org> - 4.5.0-1
- Update to 4.5.0

* Sat Apr 7 2012 Devrim GÜNDÜZ <devrim@gunduz.org> - 4.4.8-1
- Update to 4.4.8

* Tue Sep 27 2011 Devrim GÜNDÜZ <devrim@gunduz.org> - 4.4.6-2
- Fix PostgreSQL major number version. Per report from Phil Sorber .

* Tue Aug 9 2011 Devrim GUNDUZ <devrim@gunduz.org> - 4.4.6-1
- Update to 4.4.6

* Mon Apr 18 2011 Devrim GUNDUZ <devrim@gunduz.org> - 4.4.3-1
- Update to 4.4.3

* Thu Jan 6 2011 Devrim GUNDUZ <devrim@gunduz.org> - 4.4.0-3
- Add 9.0 dependency.

* Fri Mar 12 2010 Devrim GUNDUZ <devrim@gunduz.org> - 4.4.0-2
- Sync with Fedora spec again.
