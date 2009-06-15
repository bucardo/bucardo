use ExtUtils::MakeMaker;
use strict;
use warnings;
use 5.008003;

## No version.pm for this one, as the prereqs are not loaded yet.
my $VERSION = '3.2.7';

WriteMakefile(
    NAME         => 'Bucardo',
    VERSION_FROM => 'Bucardo.pm',
    PREREQ_PM    => {
                     'DBD::Pg'             => '2.0',
                     'DBI'                 => '1.51',
                     'DBIx::Safe'          => '1.2.4',
                     'ExtUtils::MakeMaker' => '6.32',
                     'Test::More'          => '0.61',
                     'Test::Harness'       => '2.03',
                     'Net::SMTP'           => '0',
                     'IO::Handle'          => '1.24',
                     'Sys::Hostname'       => '1.11',
                     'Sys::Syslog'         => '0.13',
                 },
    EXE_FILES    => ['bucardo_ctl'],
    INST_ARCHLIB => 'none',
    INST_BIN     => 'none',
    MAN1PODS     => { 'bucardo_ctl' => 'blib/man1/bucardo_ctl.1pm' },
    ABSTRACT     => 'Postgres asynchronous multi-master replication system',
    AUTHOR       => 'Greg Sabino Mullane <greg@endpoint.com>',
    LICENSE      => 'BSD',
    NO_META      => 1,
    clean        => { FILES => 'log.bucardo.* *~ *.tmp outgoingmail.* bucardo_test_database*/' },
);

package MY;
sub test {
    my $original = shift->SUPER::test(@_);
    my $PRECOM = q{$(ABSPERLRUN) t/bucardo.test.helper};
    $original =~ s/^(.*test_harness)/\t$PRECOM\n$1/m;
    return $original;
}

sub clean {
	my $string = shift->SUPER::clean(@_);
	$string =~ s{\t}{\tperl t/98-cleanup\.t\n\t};
	$string .= qq{\n\nhtml : \n\t};
    $string .= qq{\t\$(NOECHO) pod2html Bucardo.pm > Bucardo.pm.html\n};
    $string .= qq{\t\$(NOECHO) pod2html bucardo_ctl > bucardo_ctl.html\n};
    $string .= qq{\t\$(NOECHO) \$(RM_F) pod*.tmp pm_to_blib\n};
    $string .= qq{\t\$(NOECHO) \$(PERL) -pi -e "s/<link.*?>//" Bucardo.pm.html bucardo_ctl.html\n};
    $string .= qq{\t\$(NOECHO) \$(PERL) -pi -e "s~\\`\\`(.+?)''~&quot;\\1&quot;~g" Bucardo.pm.html bucardo_ctl.html\n};
    $string .= qq{\t\$(NOECHO) \$(ECHO) Created Bucardo.pm.html and bucardo_ctl.html\n};
	return $string;
}


