#!/usr/bin/perl -w
######################################################################
#
# read a list of csv files and create a sqlite3 database.
#
######################################################################
#
use strict;
#
use Carp;
use Getopt::Std;
use File::Find;
use File::Path qw(mkpath);
use File::Basename;
use File::Path 'rmtree';
use DBI;
#
######################################################################
#
# logical constants
#
use constant TRUE => 1;
use constant FALSE => 0;
#
use constant SUCCESS => 1;
use constant FAIL => 0;
#
# verbose levels
#
use constant NOVERBOSE => 0;
use constant MINVERBOSE => 1;
use constant MIDVERBOSE => 2;
use constant MAXVERBOSE => 3;
#
######################################################################
#
# globals
#
my $cmd = $0;
my $log_fh = *STDOUT;
my $dbh = undef;
#
# cmd line options
#
my $logfile = '';
my $rmv_old_db = FALSE;
my $delimiter = "\t";
my $db_path = "/tmp/CSV2DB.$$";
#
######################################################################
#
# miscellaneous functions
#
sub usage
{
    my ($arg0) = @_;
    print $log_fh <<EOF;

usage: $arg0 [-?] [-h] 
        [-l logfile]
        [-p db_path]
        [-d delimiter]
        [-r] 
        CSV-file ...

where:
    -? or -h - print this usage.
    -l logfile - log file path
    -p path - DB path. defaults to $db_path.
    -d delimiter - CSV delimiter characer. default is a tab.
    -r - remove old DB

EOF
}
#
######################################################################
#
# db functions
#
sub table_exists
{
    my ($dbh, $table_name) = @_;
    my $sth = $dbh->table_info(undef, undef, $table_name, 'TABLE');
    $sth->execute;
    my @info = $sth->fetchrow_array;
    if (scalar(@info) > 0)
    {
        return TRUE;
    }
    else
    {
        return FALSE;
    }
}
#
######################################################################
#
sub process_file
{
    my ($csv_file) = @_;
    #
    printf $log_fh "\n%d: Processing CSV File: %s\n", __LINE__, $csv_file;
    #
    # open CSV file and column names from first row.
    #
    open(my $infh, "<" , $csv_file) || die $!;
    #
    my $header = <$infh>;
    chomp($header);
    $header =~ s/\r//g;
    $header =~ s/\./_/g;
    $header =~ s/ /_/g;
    my @col_names = split /${delimiter}/, $header;
    my $num_col_names = scalar(@col_names);
    #
    # get table name
    #
    (my $tbl_name = $csv_file) =~ s/\.csv$//i;
    $tbl_name =~ s/\./_/g;
    printf $log_fh "%d: Table: %s\n", __LINE__, $tbl_name;
    #
    # check if table exists.
    #
    if (table_exists($dbh, $tbl_name) == FALSE)
    {
        printf $log_fh "%d: Creating table %s\n", __LINE__, $tbl_name;
        my $create_tbl_sql = "create table '${tbl_name}' ( '" . join("' varchar(100), '", @{col_names}) . "' varchar(100) )";
        #
        $dbh->do($create_tbl_sql);
        $dbh->commit();
    }
    #
    # generate insert sql command
    #
    my $insert_fields = "insert into '${tbl_name}' ( '" . join("','", @col_names) . "')";
    #
    my $do_commit = FALSE;
    while (my $row = <$infh>)
    {
        #
        # parse the data and remove any junk characters.
        #
        chomp($row);
        $row =~ s/\r//g;
        my @data = split /${delimiter}/, $row;
        my $insert_sql = $insert_fields . " values ( '" . join("','", @data) . "')";
        if ( ! eval { $dbh->do($insert_sql); 1; } ) 
        {
            printf $log_fh "%d: ERROR INSERT FAILED: %s\n", __LINE__, $@;
        }
        else
        {
            $do_commit = TRUE;
        }
    }
    #
    $dbh->commit() if ($do_commit == TRUE);
    #
    return;
}
#
######################################################################
#
my %opts;
if (getopts('?hp:l:d:r', \%opts) != 1)
{
    usage($cmd);
    exit 2;
}
#
foreach my $opt (%opts)
{
    if (($opt eq 'h') or ($opt eq '?'))
    {
        usage($cmd);
        exit 0;
    }
    elsif ($opt eq 'p')
    {
        $db_path = $opts{$opt};
        printf $log_fh "\n%d: DB path: %s\n", __LINE__, $db_path;
    }
    elsif ($opt eq 'r')
    {
        $rmv_old_db = TRUE;
    }
    elsif ($opt eq 'l')
    {
        local *FH;
        $logfile = $opts{$opt};
        open(FH, '>', $logfile) or die $!;
        $log_fh = *FH;
        printf $log_fh "\n%d: Log File: %s\n", __LINE__, $logfile;
    }
    elsif ($opt eq 'd')
    {
        $delimiter = $opts{$opt};
        $delimiter = "\t" if ( $delimiter =~ /^$/ );
    }
}
#
if (scalar(@ARGV) == 0)
{
    printf $log_fh "\n%d: No CSV files given.\n", __LINE__;
    usage($cmd);
    exit 2;
}
#
# check if remove old data.
#
unlink($db_path) if ($rmv_old_db == TRUE);
#
# create db if needed.
#
if ( ! -f $db_path )
{
    printf $log_fh "\n%d: Using new DB: %s.\n", __LINE__, $db_path;
}
else
{
    printf $log_fh "\n%d: Re-using existing DB: %s.\n", __LINE__, $db_path;
}
my $dsn = "dbi:SQLite:dbname=${db_path}";
my $user = "";
my $password = "";
$dbh = DBI->connect($dsn,
                    $user,
                    $password,
                    {
                        PrintError => 0,
                        RaiseError => 1,
                        AutoCommit => 0,
                        FetchHashKeyName => 'NAME_lc'
                    });
#
# process each file and place data into db.
#
if ( -t STDIN )
{
    #
    # getting a list of files from command line.
    #
    if (scalar(@ARGV) == 0)
    {
        printf $log_fh "%d: No csv files given.\n", __LINE__;
        usage($cmd);
        exit 2;
    }
    #
    foreach my $csv_file (@ARGV)
    {
        process_file($csv_file);
    }
}
else
{
    printf $log_fh "%d: Reading STDIN for list of files ...\n", __LINE__;
    while( defined(my $csv_file = <STDIN>) )
    {
        chomp($csv_file);
        process_file($csv_file);
    }
}
#
# close db
#
printf $log_fh "\n%d: Closing DB: %s.\n", __LINE__, $db_path;
$dbh->disconnect;
#
exit 0;
