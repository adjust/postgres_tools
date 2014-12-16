use 5.012;
use Module::Build;

my $build = Module::Build->new(
    module_name       => 'PostgresTools',
    dist_abstract     => 'Backup and Restore scripts for PostgreSQL',
    dist_author       => 'Robert Abraham <roa@adeven.com>',
    dist_version_from => 'lib/PostgresTools.pm',
    license           => 'mit',
    build_requires    => { 'Module::Build' => 0.38, },
    requires          => {
        'Moo'                        => 0,
        'DBD::Pg'                    => 0,
        'Getopt::Long'               => 0,
        'File::Path'                 => 0,
        'File::Rsync'                => 0,
        'DateTime'                   => 0,
        'DateTime::Format::Strptime' => 0,
        'Parallel::ForkManager'      => 0,
    },
    script_files => [ 'bin/dump.pl', 'bin/dump93.pl', 'bin/restore.pl', 'bin/restore93.pl', 'bin/diff.pl', 'bin/analyze.pl' ],
);

$build->create_build_script;
