use strict;
use 5.008003;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'Bucardo',
    VERSION_FROM => 'Bucardo.pm',
    PREREQ_PM    => {
                     'DBD::Pg'             => '1.49',
                     'DBI'                 => '1.51',
                     'DBIx::Safe'          => '1.2.4',
                     'ExtUtils::MakeMaker' => '6.32',
                     'Test::More'          => '0.61',
                     'Test::Harness'       => '2.03',
                     'Mail::Sendmail'      => '0.79',
                     'Moose'               => '0.18',
                     'IO::Handle'          => '1.24',
                     'Sys::Hostname'       => '1.11',
                     'Sys::Syslog'         => '0.13',
                 },
    ABSTRACT     => 'Postgres asynchronous multi-master replication system',
    AUTHOR       => 'Greg Sabino Mullane <greg@endpoint.com>',
    LICENSE      => 'BSD',
    NO_META      => 1,
    clean        => { FILES => 'log.bucardo.* *~ *.tmp outgoingmail.*' },
);


package MY;
sub test {
	my $original = shift->SUPER::test(@_);
	my $PRECOM = q{$(ABSPERLRUN) t/bucardo.test.helper};
	$original =~ s/^(.*test_harness)/\t$PRECOM\n$1/m;
	return $original;
}

sub manifypods {
	my $after = qq{\t\$(NOECHO) pod2html Bucardo.pm > Bucardo.pm.html\n};
	$after .= qq{\t\$(NOECHO) pod2html bucardo_ctl > bucardo_ctl.html\n};
	$after .= qq{\t\$(NOECHO) \$(RM_F) pod*.tmp pm_to_blib\n};
	$after .= qq{\t\$(NOECHO) \$(PERL) -pi -e "s/<link.*?>//" Bucardo.pm.html bucardo_ctl.html\n};
	$after .= qq{\t\$(NOECHO) \$(ECHO) Created Bucardo.pm.html and bucardo_ctl.html\n};
	return shift->SUPER::manifypods(@_) . $after;
}
