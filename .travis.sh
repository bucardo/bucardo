#!/bin/bash

set -eu

client_configure() {
    # noop
    echo -n
}

remove_old_pgdg() {
	sudo rm /etc/apt/sources.list.d/pgdg.list
}

pgdg_repository() {
	local sourcelist='sources.list.d/postgresql.list'

	curl -sS 'https://www.postgresql.org/media/keys/ACCC4CF8.asc' | sudo apt-key add -
	echo deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main $PGVERSION | sudo tee "/etc/apt/$sourcelist"
	sudo apt-get -o Dir::Etc::sourcelist="$sourcelist" -o Dir::Etc::sourceparts='-' -o APT::Get::List-Cleanup='0' update
}

postgresql_configure() {
	sudo tee /etc/postgresql/$PGVERSION/main/pg_hba.conf > /dev/null <<-config
		local     all         all                               trust
		host      all         all         127.0.0.1/32          trust
		host      all         all         ::1/128               trust
	config

	sort -VCu <<-versions ||
		$PGVERSION
		9.2
	versions

	echo 127.0.0.1 postgres | sudo tee -a /etc/hosts > /dev/null

	sudo service postgresql restart
}

postgresql_install() {
	xargs sudo apt-get -y -o Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confnew' install <<-packages
		postgresql-$PGVERSION
		postgresql-server-dev-$PGVERSION
		postgresql-contrib-$PGVERSION
		postgresql-plperl-$PGVERSION
		libdbd-pg-perl
		perl-dbdabi-94
		libdbi-perl
		clang
		llvm-6.0-dev
		libencode-locale-perl
	packages
}

postgresql_uninstall() {
	sudo service postgresql stop
	xargs sudo apt-get -y --purge remove <<-packages
		libpq-dev
		libpq5
		postgresql
		postgresql-client-common
		postgresql-common
	packages
	sudo rm -rf /var/lib/postgresql
}

$1
