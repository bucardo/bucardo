%define realname Bucardo
%define sysuser bucardo
%define sysgroup %sysuser
%define servicename %{name}.service

Name:      bucardo
Version:   5.7.0
Release:   1%{?dist}
Summary:   PostgreSQL replication system for multi-master and multi-replica operations

Group:     Applications/Databases
License:   BSD
URL:       https://bucardo.org/
Source0:   https://bucardo.org/downloads/Bucardo-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Source1:   bucardo.service
Source2:   bucardorc
Patch0:    bucardo-logfiles.patch

BuildArch: noarch

%systemd_requires

BuildRequires: make
BuildRequires: perl(base)
BuildRequires: perl(Cwd)
BuildRequires: perl(Exporter)
BuildRequires: perl(ExtUtils::MakeMaker) >= 6.68
BuildRequires: perl-generators
BuildRequires: perl(lib)
BuildRequires: perl(Test::Harness)
BuildRequires: perl(Test::More)

Requires(pre): shadow-utils

Requires: perl(:MODULE_COMPAT_%(eval "$(perl -V:version)"; echo $version))
Requires: perl(base)
Requires: perl(boolean)
Requires: perl(DBD::Pg)
Requires: perl(DBI)
Requires: perl(DBIx::Safe)
Requires: perl(IO::Handle)
Requires: perl(List::Util)
Requires: perl(Net::SMTP)
Requires: perl(open)
Requires: perl(Pod::PlainText)
Requires: perl(Pod::Usage)
Requires: perl(Sys::Hostname)
Requires: perl(Sys::Syslog)
Requires: postgresql-libs

# These aren't required to be on the same server as Bucardo, but most will want them
%if 0%{?fedora} || 0%{?rhel} > 7
Recommends: postgresql
Recommends: postgresql-plperl
%else
Requires: postgresql
Requires: postgresql-plperl
%endif

# Optional and less commonly used, so disable because they would be installed
# by default unless using `dnf --setopt=install_weak_deps=False`
#Recommends: perl(DBD::mysql)
#Recommends: perl(DBD::SQLite)
#Recommends: perl(MongoDB)
#Recommends: perl(Redis)

%description
Bucardo is an asynchronous PostgreSQL replication system, allowing for
multi-master and multi-replica operations. It was developed at Backcountry.com
primarily by Greg Sabino Mullane of End Point Corporation.

%pre
getent group %sysgroup >/dev/null || groupadd -r %sysgroup
getent passwd %sysuser >/dev/null || useradd -r -g %sysgroup -d /var/lib/bucardo -m -k /dev/null -s /sbin/nologin -c "Bucardo replication" %sysuser
mkdir -p /var/log/bucardo
chown -R %{sysuser}:%{sysgroup} /var/log/bucardo
chmod o= /var/log/bucardo

%prep
%setup -q -n %{realname}-%{version}
%patch0 -p0

%build

%{__perl} Makefile.PL INSTALLDIRS=vendor NO_PACKLIST=1

make %{?_smp_mflags}

%install
rm -rf %{buildroot}

make pure_install PERL_INSTALL_ROOT=%{buildroot}

find %{buildroot} -type f -name .packlist -exec rm -f {} +

sed -i -e '1d;2i#!%{__perl}' bucardo

rm -f %{buildroot}/%{_bindir}/bucardo
install -Dp -m 755 bucardo %{buildroot}%{_sbindir}/%{name}

install -d %{buildroot}/%{_unitdir}
install -m 755 %{SOURCE1} %{buildroot}%{_unitdir}/%{servicename}

install -d %{buildroot}/%{_sysconfdir}
install -m 750 %{SOURCE2} %{buildroot}%{_sysconfdir}/bucardorc

%{_fixperms} %{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%license LICENSE
%doc *.html Changes INSTALL README TODO
%config %{_sysconfdir}/bucardorc
%{_unitdir}/%{servicename}
%{perl_vendorlib}/*
%{_mandir}/man1/*
%{_mandir}/man3/*
%{_sbindir}/%{name}
%{_datadir}/%{name}/bucardo.schema

%changelog
* Fri Apr 16 2021 Jon Jensen <jon@endpoint.com> - 5.7.0-1
- Update to 5.7.0.
- Adapt for RHEL 7/8.
- Use some improvements from Fedora specfile.
- Replace SysV init script with a new systemd unit file that uses Bucardo's new foreground mode.
- Leave creating /var/run/bucardo to systemd unit file, since /var/run -> /run is now ephemeral.
- Create a bucardo user to run under and install /etc/bucardorc config file to use new runuser option by default.

* Tue Jan 19 2021 Jon Jensen <jon@endpoint.com> - 5.6.0-1
- Remove references to nonexistent master-master-replication-example.txt
  documentation file, a relic of the spec file this was started from.
  (Fixes GitHub issue #149.)
- Update to 5.6.0.

* Tue Nov 1 2016 David Christensen <david@endpoint.com> - 5.4.1-1
- Update to 5.4.1

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
