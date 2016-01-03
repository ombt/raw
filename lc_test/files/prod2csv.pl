#!/usr/bin/perl -w
######################################################################
#
# process product file (MAI or CRB) and create CSV files for each section.
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
# section names
#
use constant INDEX => '[Index]';
use constant INFORMATION => '[Information]';
use constant LOTNAMES => '[LotNames]';
#
# verbose levels
#
use constant NOVERBOSE => 0;
use constant MINVERBOSE => 1;
use constant MIDVERBOSE => 2;
use constant MAXVERBOSE => 3;
#
# section types
#
use constant SECTION_UNKNWON => 0;
use constant SECTION_NAME_VALUE => 1;
use constant SECTION_LIST => 2;
#
# file types
#
use constant FILE_TYPE_CRB => 0;
use constant FILE_TYPE_MAI => 1;
use constant FILE_TYPE_UNKNOWN => 2; # always last
#
my %file_types =
(
    'cerberus' => FILE_TYPE_CRB(),
    'maihime2' => FILE_TYPE_MAI()
);
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
my $verbose = NOVERBOSE;
my $rmv_prod_dir = FALSE;
my $delimiter = "\t";
my $combine_lot_files = FALSE;
my $save_to_db = FALSE;
my $save_to_csv = TRUE;
#
# check for preferred product db/csv directory if it exists, else
# use the default. this may ne overwritten by using a command-line option.
#
my $product_data_dir = undef;
$product_data_dir = $ENV{'PRODUCT_DATA_DIR'} 
    if (exists($ENV{'PRODUCT_DATA_DIR'}));
$product_data_dir = './CRB_MAI_PROD_DATA/' 
    unless (defined($product_data_dir) and ($product_data_dir ne ""));
#
my %verbose_levels =
(
    off => NOVERBOSE(),
    min => MINVERBOSE(),
    mid => MIDVERBOSE(),
    max => MAXVERBOSE()
);
#
######################################################################
#
# miscellaneous functions
#
sub usage
{
    my ($arg0) = @_;
    print $log_fh <<EOF;

usage: $arg0 [-?] [-h]  \\ 
        [-w | -W |-v level] \\ 
        [-l logfile] \\ 
        [-p path] \\
        [-d delimiter] \\
        [-R] [-L] [-D] \\
        CRB or MAI file ...

where:
    -? or -h - print this usage.
    -w - enable warning (level=min=1)
    -W - enable warning and trace (level=mid=2)
    -v - verbose level: 0=off,1=min,2=mid,3=max
    -l logfile - log file path
    -p path - csv and db directory, defaults to './CSV'.
    -d delimiter - CSV delimiter characer. default is a tab.
    -R - remove old CRB or MAI directories (off by default).
    -L - combine separate LOT files into one file keyed by LOT.
    -D - export to SQLite DB

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
# load name-value or list section
#
sub load_name_value
{
    my ($praw_data, $section, $pirec, $max_rec, $pprod_db) = @_;
    #
    $pprod_db->{found_data}->{$section} = FALSE;
    $pprod_db->{section_type}->{$section} = SECTION_NAME_VALUE;
    #
    my $re_section = '\\' . $section;
    my @section_data = 
        grep /^${re_section}\s*$/ .. /^\s*$/, @{$praw_data};
    #
    printf $log_fh "%d: <%s>\n", 
        __LINE__, 
        join("\n", @section_data) 
        if ($verbose >= MAXVERBOSE);
    #
    $$pirec += scalar(@section_data);
    #
    if (scalar(@section_data) <= 2)
    {
        $pprod_db->{$section} = {};
        printf $log_fh "\t\t%d: NO NAME-VALUE DATA FOUND IN SECTION %s. Lines read: %d\n", 
            __LINE__, $section, scalar(@section_data);
        return FAIL;
    }
    #
    shift @section_data; # remove section name
    pop @section_data;   # remove end-of-section null-length line
    #
    %{$pprod_db->{$section}->{data}} = 
        map { split /\s*=\s*/, $_, 2 } @section_data;
    #
    $pprod_db->{found_data}->{$section} = TRUE;
    #
    printf $log_fh "\t\t%d: Number of key-value pairs: %d\n", 
        __LINE__, 
        scalar(keys %{$pprod_db->{$section}->{data}})
        if ($verbose >= MINVERBOSE);
    printf $log_fh "\t\t%d: Lines read: %d\n", 
        __LINE__, 
        scalar(@section_data)
        if ($verbose >= MINVERBOSE);
    #
    return SUCCESS;
}
#
sub split_quoted_string
{
    my $rec = shift;
    #
    my $rec_len = length($rec);
    #
    my $istart = -1;
    my $iend = -1;
    my $in_string = 0;
    #
    my @tokens = ();
    my $token = "";
    #
    for (my $i=0; $i<$rec_len; $i++)
    {
        my $c = substr($rec, $i, 1);
        #
        if ($in_string == 1)
        {
            if ($c eq '"')
            {
                $in_string = 0;
            }
            else
            {
                $token .= $c;
            }
        }
        elsif ($c eq '"')
        {
            $in_string = 1;
        }
        elsif ($c eq ' ')
        {
            # printf $log_fh "Token ... <%s>\n", $token;
            push (@tokens, $token);
            $token = '';
        }
        else
        {
            $token .= $c;
        }
    }
    #
    if (length($token) > 0)
    {
        # printf $log_fh "Token ... <%s>\n", $token;
        push (@tokens, $token);
        $token = '';
    }
    else
    {
        # null-length string
        $token = '';
        push (@tokens, $token);
    }
    #
    # printf $log_fh "Tokens: \n%s\n", join("\n",@tokens);
    #
    return @tokens;
}
#
sub load_list
{
    my ($praw_data, $section, $pirec, $max_rec, $pprod_db) = @_;
    #
    $pprod_db->{found_data}->{$section} = FALSE;
    $pprod_db->{section_type}->{$section} = SECTION_LIST;
    #
    my $re_section = '\\' . $section;
    my @section_data = 
        grep /^${re_section}\s*$/ .. /^\s*$/, @{$praw_data};
    #
    printf $log_fh "%d: <%s>\n", __LINE__, join("\n", @section_data) if ($verbose >= MAXVERBOSE);
    #
    $$pirec += scalar(@section_data);
    #
    if (scalar(@section_data) <= 3)
    {
        $pprod_db->{$section} = {};
        printf $log_fh "\t\t\t%d: NO LIST DATA FOUND IN SECTION %s. Lines read: %d\n", 
            __LINE__, 
            $section, scalar(@section_data)
            if ($verbose >= MINVERBOSE);
        return SUCCESS;
    }
    #
    shift @section_data; # remove section name
    pop @section_data;   # remove end-of-section null-length line
    #
    $pprod_db->{$section}->{header} = shift @section_data;
    @{$pprod_db->{$section}->{column_names}} = 
        split / /, $pprod_db->{$section}->{header};
    my $number_columns = scalar(@{$pprod_db->{$section}->{column_names}});
    #
    @{$pprod_db->{$section}->{data}} = ();
    #
    printf $log_fh "\t\t\t%d: Number of Columns: %d\n", 
        __LINE__, 
        $number_columns
        if ($verbose >= MINVERBOSE);
    #
    foreach my $record (@section_data)
    {
        #
        # sanity check since MAI or CRB file may be corrupted.
        #
        last if (($record =~ m/^\[[^\]]*\]/) ||
                 ($record =~ m/^\s*$/));
        #
        my @tokens = split_quoted_string($record);
        my $number_tokens = scalar(@tokens);
        #
        printf $log_fh "\t\t\t%d: Number of tokens in record: %d\n", __LINE__, $number_tokens if ($verbose >= MAXVERBOSE);
        #
        if ($number_tokens == $number_columns)
        {
            my %data = ();
            @data{@{$pprod_db->{$section}->{column_names}}} = @tokens;
            #
            unshift @{$pprod_db->{$section}->{data}}, \%data;
            printf $log_fh "\t\t\t%d: Current Number of Records: %d\n", __LINE__, scalar(@{$pprod_db->{$section}->{data}}) if ($verbose >= MAXVERBOSE);
        }
        else
        {
            printf $log_fh "\t\t\t%d: Section: %s, SKIPPING RECORD - NUMBER TOKENS (%d) != NUMBER COLUMNS (%d)\n", __LINE__, $section, $number_tokens, $number_columns;
        }
    }
    #
    $pprod_db->{found_data}->{$section} = TRUE;
    #
    return SUCCESS;
}
#
######################################################################
#
# load and process product files, either CRB or MAI
#
sub read_file
{
    my ($prod_file, $praw_data) = @_;
    #
    printf $log_fh "\t%d: Reading Product file: %s\n", 
                   __LINE__, $prod_file;
    #
    if ( ! -r $prod_file )
    {
        printf $log_fh "\t%d: ERROR: file $prod_file is NOT readable\n\n", __LINE__;
        return FAIL;
    }
    #
    unless (open(INFD, $prod_file))
    {
        printf $log_fh "\t%d: ERROR: unable to open $prod_file.\n\n", __LINE__;
        return FAIL;
    }
    @{$praw_data} = <INFD>;
    close(INFD);
    #
    # remove any CR-NL sequences from Windose.
    chomp(@{$praw_data});
    s/\r//g for @{$praw_data};
    #
    printf $log_fh "\t\t%d: Lines read: %d\n", __LINE__, scalar(@{$praw_data}) if ($verbose >= MINVERBOSE);
    #
    return SUCCESS;
}
#
sub process_data
{
    my ($prod_file, $praw_data, $pprod_db) = @_;
    #
    printf $log_fh "\t%d: Processing product data: %s\n", 
                   __LINE__, $prod_file;
    #
    my $max_rec = scalar(@{$praw_data});
    my $sec_no = 0;
    #
    for (my $irec=0; $irec<$max_rec; )
    {
        my $rec = $praw_data->[$irec];
        #
        if ($rec =~ m/^(\[[^\]]*\])/)
        {
            my $section = ${1};
            #
            printf $log_fh "\t\t%d: Section %03d: %s\n", 
                __LINE__, ++$sec_no, $section
                if ($verbose >= MINVERBOSE);
            #
            $rec = $praw_data->[${irec}+1];
            #
            if ($rec =~ m/^\s*$/)
            {
                $irec += 2;
                printf $log_fh "\t\t%d: Empty section - %s\n", 
                               __LINE__, $section;
            }
            elsif ($rec =~ m/.*=.*/)
            {
                load_name_value($praw_data, 
                                $section, 
                               \$irec, 
                                $max_rec,
                                $pprod_db);
            }
            else
            {
                load_list($praw_data, 
                          $section, 
                         \$irec, 
                          $max_rec,
                          $pprod_db);
            }
        }
        else
        {
            $irec += 1;
        }
    }
    #
    return SUCCESS;
}
#
sub export_list_to_csv
{
    my ($prod_file, $pprod_db, $prod_dir, $section, $ftype) = @_;
    #
    if (($combine_lot_files == FALSE) ||
        ($section !~ m/<([0-9]+)>/))
    {
        my $csv_file = $section;
        $csv_file =~ s/[\[\]]//g;
        $csv_file =~ s/<([0-9]+)>/_$1/g;
        #
        my $outnm = $prod_dir . '/' . $csv_file . ".csv";
        #
        my $print_cols = FALSE;
        $print_cols = TRUE if ( ! -r $outnm );
        #
        open(my $outfh, "+>>" , $outnm) || die $!;
        #
        my $pcols = $pprod_db->{$section}->{column_names};
        if ($print_cols == TRUE)
        {
            my $comma = "";
            foreach my $col (@{$pcols})
            {
                printf $outfh "%s%s", $comma, $col;
                $comma = $delimiter;
            }
            printf $outfh "\n";
        }
        #
        foreach my $prow (@{$pprod_db->{$section}->{data}})
        {
            my $comma = "";
            foreach my $col (@{$pcols})
            {
                printf $outfh "%s%s", $comma, $prow->{$col};
                $comma = $delimiter;
            }
            printf $outfh "\n";
        }
        #
        close($outfh);
    }
    else
    {
        my $csv_file = $section;
        $csv_file =~ s/[\[\]]//g;
        $csv_file =~ s/<([0-9]+)>//g;
        my $lotno = $1;
        #
        my $outnm = $prod_dir . '/' . $csv_file . ".csv";
        #
        my $print_cols = FALSE;
        $print_cols = TRUE if ( ! -r $outnm );
        #
        open(my $outfh, "+>>" , $outnm) || die $!;
        #
        my $pcols = $pprod_db->{$section}->{column_names};
        if ($print_cols == TRUE)
        {
            printf $outfh "lotno";
            foreach my $col (@{$pcols})
            {
                printf $outfh "%s%s", $delimiter, $col;
            }
            printf $outfh "\n";
        }
        #
        foreach my $prow (@{$pprod_db->{$section}->{data}})
        {
            printf $outfh "%s", $lotno;
            foreach my $col (@{$pcols})
            {
                printf $outfh "%s%s", $delimiter, $prow->{$col};
            }
            printf $outfh "\n";
        }
        #
        close($outfh);
    }
}
#
sub export_name_value_to_csv
{
    my ($prod_file, $pprod_db, $prod_dir, $section, $ftype) = @_;
    #
    if (($combine_lot_files == FALSE) ||
        ($section !~ m/<([0-9]+)>/))
    {
        my $csv_file = $section;
        $csv_file =~ s/[\[\]]//g;
        $csv_file =~ s/<([0-9]+)>/_$1/g;
        #
        my $outnm = $prod_dir . '/' . $csv_file . ".csv";
        #
        my $print_cols = FALSE;
        $print_cols = TRUE if ( ! -r $outnm );
        #
        open(my $outfh, "+>>" , $outnm) || die $!;
        #
        if ($print_cols == TRUE)
        {
            printf $outfh "NAME%sVALUE\n", $delimiter;
        }
        #
        foreach my $key (keys %{$pprod_db->{$section}->{data}})
        {
            printf $outfh "%s%s%s\n", 
                $key, 
                $delimiter,
                $pprod_db->{$section}->{data}->{$key};
        }
        #
        close($outfh);
    }
    else
    {
        my $csv_file = $section;
        $csv_file =~ s/[\[\]]//g;
        $csv_file =~ s/<([0-9]+)>//g;
        my $lotno = $1;
        #
        my $outnm = $prod_dir . '/' . $csv_file . ".csv";
        #
        my $print_cols = FALSE;
        $print_cols = TRUE if ( ! -r $outnm );
        #
        open(my $outfh, "+>>" , $outnm) || die $!;
        #
        if ($print_cols == TRUE)
        {
            printf $outfh "LOTNO%sNAME%sVALUE\n", $delimiter, $delimiter;
        }
        #
        foreach my $key (keys %{$pprod_db->{$section}->{data}})
        {
            printf $outfh "%s%s%s%s%s\n", 
                $lotno, 
                $delimiter,
                $key, 
                $delimiter,
                $pprod_db->{$section}->{data}->{$key};
        }
        #
        close($outfh);
    }
}
#
sub get_file_type
{
    my ($pprod_db) = @_;
    #
    my $file_type = FILE_TYPE_UNKNOWN;
    if (exists($pprod_db->{'[Index]'}->{data}->{Format}))
    {
        my $format = $pprod_db->{'[Index]'}->{data}->{Format};
        foreach my $re (keys %file_types)
        {
            if ($format =~ m/\Q$re/i)
            {
                $file_type = $file_types{$re};
                printf $log_fh "\t\t%d: File type: %s\n", 
                               __LINE__, $re;
                last;
            }
        }
    }
    if ($file_type == FILE_TYPE_UNKNOWN)
    {
        printf $log_fh "\t\t%d: File type: UNKNOWN\n", __LINE__;
    }
    return $file_type;
}
#
sub export_to_csv
{
    my ($prod_file, $pprod_db) = @_;
    #
    printf $log_fh "\t%d: Writing product data to CSV: %s\n", 
                   __LINE__, $prod_file;
    #
    my $prod_name = basename($prod_file);
    $prod_name =~ tr/a-z/A-Z/;
    my $prod_csv_dir = $product_data_dir . '/CSV_' . $prod_name;
    #
    rmtree($prod_csv_dir) if ($rmv_prod_dir == TRUE);
    ( mkpath($prod_csv_dir) || die $! ) unless ( -d $prod_csv_dir );
    #
    printf $log_fh "\t\t%d: product %s CSV directory: %s\n", 
        __LINE__, $prod_name, $prod_csv_dir;
    #
    my $file_type = get_file_type($pprod_db);
    #
    foreach my $section (sort keys %{$pprod_db->{found_data}})
    {
        if ($pprod_db->{found_data}->{$section} != TRUE)
        {
            printf $log_fh "\t\t%d: No data for section %s. Skipping it.\n", 
                __LINE__, $section if ($verbose >= MINVERBOSE);
        }
        elsif ($pprod_db->{section_type}->{$section} == SECTION_NAME_VALUE)
        {
            printf $log_fh "\t\t%d: Name-Value Section: %s\n", 
                __LINE__, $section;
            export_name_value_to_csv($prod_file,
                                     $pprod_db,
                                     $prod_csv_dir,
                                     $section,
                                     $file_type);
        }
        elsif ($pprod_db->{section_type}->{$section} == SECTION_LIST)
        {
            printf $log_fh "\t\t%d: List Section: %s\n", 
                __LINE__, $section;
            export_list_to_csv($prod_file,
                               $pprod_db,
                               $prod_csv_dir,
                               $section,
                               $file_type);
        }
        else
        {
            printf $log_fh "\t\t%d: Unknown type Section: %s\n", 
                __LINE__, $section;
        }
    }
    #
    return SUCCESS;
}
#
sub export_list_to_db
{
    my ($prod_file, $pprod_db, $prod_dir, $section, $ftype) = @_;
    #
    if (($combine_lot_files == FALSE) ||
        ($section !~ m/<([0-9]+)>/))
    {
        #
        # generate table name and verify if table exists. if table
        # does not exist, then create the table.
        #
        my $table_name = $section . '_LSEC';
        $table_name =~ s/[\[\]]//g;
        $table_name =~ s/<([0-9]+)>/_$1/g;
        #
        my $dbh = $pprod_db->{sqlite}->{dbh};
        #
        if (table_exists($dbh, $table_name) == TRUE)
        {
            printf $log_fh "\t\t\t%d: Table %s already exists\n", __LINE__, $table_name;
        }
        else
        {
            printf $log_fh "\t\t\t%d: Creating table %s\n", __LINE__, $table_name;
            # $create_tbl_sql .= ' )';
            my $pcols = $pprod_db->{$section}->{column_names};
            my $create_tbl_sql = "create table ${table_name} ( '" . join("' varchar(100), '", @{$pcols}) . "' varchar(100) )";
            #
            $dbh->do($create_tbl_sql);
            $dbh->commit();
        }
        #
        # generate insert sql command
        #
        my $pcols = $pprod_db->{$section}->{column_names};
        my $insert_fields = "insert into ${table_name} ( '" . join("','", @{$pcols}) . "')";
        #
        my $do_commit = FALSE;
        foreach my $prow (@{$pprod_db->{$section}->{data}})
        {
            my $insert_sql = $insert_fields . " values ( '" . join("','", @{$prow}{@{$pcols}}) . "')";
            if ( ! eval { $dbh->do($insert_sql); 1; } ) 
            {
                printf $log_fh "\t\t\t%d: ERROR INSERT FAILED: %s\n", __LINE__, $@;
            }
            else
            {
                $do_commit = TRUE;
            }
        }
        $dbh->commit() if ($do_commit == TRUE);
    }
    else
    {
        #
        # generate table name and verify if table exists. if table
        # does not exist, then create the table.
        #
        # combine lots into one table.
        #
        my $table_name = $section . '_LSEC';
        $table_name =~ s/[\[\]]//g;
        $table_name =~ s/<([0-9]+)>//g;
        my $lotno = $1;
        #
        my $dbh = $pprod_db->{sqlite}->{dbh};
        #
        if (table_exists($dbh, $table_name) == TRUE)
        {
            printf $log_fh "\t\t\t%d: Table %s already exists\n", __LINE__, $table_name;
        }
        else
        {
            printf $log_fh "\t\t\t%d: Creating table %s\n", __LINE__, $table_name;
            # $create_tbl_sql .= ' )';
            my $pcols = $pprod_db->{$section}->{column_names};
            my $create_tbl_sql = "create table ${table_name} ( 'lotno' varchar(100), '" . join("' varchar(100), '", @{$pcols}) . "' varchar(100) )";
            #
            $dbh->do($create_tbl_sql);
            $dbh->commit();
        }
        #
        # generate insert sql command
        #
        my $pcols = $pprod_db->{$section}->{column_names};
        my $insert_fields = "insert into ${table_name} ( 'lotno', '" . join("','", @{$pcols}) . "')";
        #
        my $do_commit = FALSE;
        foreach my $prow (@{$pprod_db->{$section}->{data}})
        {
            my $insert_sql = $insert_fields . " values ( '${lotno}', '" . join("','", @{$prow}{@{$pcols}}) . "')";
            if ( ! eval { $dbh->do($insert_sql); 1; } ) 
            {
                printf $log_fh "\t\t\t%d: ERROR INSERT FAILED: %s\n", __LINE__, $@;
            }
            else
            {
                $do_commit = TRUE;
            }
        }
        $dbh->commit() if ($do_commit == TRUE);
    }
    #
    return;
}
#
sub export_name_value_to_db
{
    my ($prod_file, $pprod_db, $prod_dir, $section, $ftype) = @_;
    #
    if (($combine_lot_files == FALSE) ||
        ($section !~ m/<([0-9]+)>/))
    {
        #
        # generate table name and verify if table exists. if table
        # does not exist, then create the table.
        #
        my $table_name = $section . '_NVSEC';
        $table_name =~ s/[\[\]]//g;
        $table_name =~ s/<([0-9]+)>/_$1/g;
        #
        my $dbh = $pprod_db->{sqlite}->{dbh};
        #
        if (table_exists($dbh, $table_name) == TRUE)
        {
            printf $log_fh "\t\t\t%d: Table %s already exists\n", __LINE__, $table_name;
        }
        else
        {
            printf $log_fh "\t\t\t%d: Creating table %s\n", __LINE__, $table_name;
            my $create_tbl_sql = sprintf <<'END_SQL', ${table_name};
create table %s (
    name varchar(100) not null,
    value varchar(100) not null,
    primary key (name)
)
END_SQL
            #
            $dbh->do($create_tbl_sql);
            $dbh->commit();
        }
        #
        my $insert_sql = "insert into ${table_name} (name, value) values (?, ?)";
        #
        my $do_commit = FALSE;
        foreach my $key (keys %{$pprod_db->{$section}->{data}})
        {
            if ( ! eval { $dbh->do($insert_sql, undef, $key, $pprod_db->{$section}->{data}->{$key}); 1; } )
            {
                printf $log_fh "\t\t\t%d: ERROR INSERT FAILED: %s\n", __LINE__, $@;
            }
            else
            {
                $do_commit = TRUE;
            }
        }
        $dbh->commit() if ($do_commit == TRUE);
    }
    else
    {
        #
        # generate table name and verify if table exists. if table
        # does not exist, then create the table.
        #
        # combine lots into one table.
        #
        my $table_name = $section . '_NVSEC';
        $table_name =~ s/[\[\]]//g;
        $table_name =~ s/<([0-9]+)>//g;
        my $lotno = $1;
        #
        my $dbh = $pprod_db->{sqlite}->{dbh};
        #
        if (table_exists($dbh, $table_name) == TRUE)
        {
            printf $log_fh "\t\t\t%d: Table %s already exists\n", __LINE__, $table_name;
        }
        else
        {
            printf $log_fh "\t\t\t%d: Creating table %s\n", __LINE__, $table_name;
            my $create_tbl_sql = sprintf <<'END_SQL', ${table_name};
create table %s (
    lotno integer not null,
    name varchar(100) not null,
    value varchar(100) not null,
    primary key (lotno, name)
)
END_SQL
            #
            $dbh->do($create_tbl_sql);
            $dbh->commit();
        }
        #
        my $insert_sql = "insert into ${table_name} (lotno, name, value) values (?, ?, ?)";
        #
        my $do_commit = FALSE;
        foreach my $key (keys %{$pprod_db->{$section}->{data}})
        {
            if ( ! eval { $dbh->do($insert_sql, undef, $lotno, $key, $pprod_db->{$section}->{data}->{$key}); 1; } )
            {
                printf $log_fh "\t\t\t%d: ERROR INSERT FAILED: %s\n", __LINE__, $@;
            }
            else
            {
                $do_commit = TRUE;
            }
        }
        $dbh->commit() if ($do_commit == TRUE);
    }
}
#
sub export_to_db
{
    my ($prod_file, $pprod_db) = @_;
    #
    printf $log_fh "\t%d: Writing product data to DB: %s\n", 
                   __LINE__, $prod_file;
    #
    my $prod_name = basename($prod_file);
    $prod_name =~ tr/a-z/A-Z/;
    my $prod_db_path = $product_data_dir . '/DB_' . $prod_name;
    #
    unlink($prod_db_path) if ($rmv_prod_dir == TRUE);
    #
    printf $log_fh "\t\t%d: product %s DB file: %s\n", 
        __LINE__, $prod_name, $prod_db_path;
    #
    printf $log_fh "\t\t%d: Creating DB: %s\n", 
        __LINE__, $prod_db_path;
    #
    $pprod_db->{sqlite}->{dsn} = "dbi:SQLite:dbname=${prod_db_path}";
    $pprod_db->{sqlite}->{user} = "";
    $pprod_db->{sqlite}->{password} = "";
    $pprod_db->{sqlite}->{dbh} = 
        DBI->connect(
            $pprod_db->{sqlite}->{dsn},
            $pprod_db->{sqlite}->{user},
            $pprod_db->{sqlite}->{password},
            {
                PrintError => 0,
                RaiseError => 1,
                AutoCommit => 0,
                FetchHashKeyName => 'NAME_lc'
            });
    #
    my $file_type = get_file_type($pprod_db);
    #
    foreach my $section (sort keys %{$pprod_db->{found_data}})
    {
        if ($pprod_db->{found_data}->{$section} != TRUE)
        {
            printf $log_fh "\t\t%d: No data for section %s. Skipping it.\n", 
                __LINE__, $section if ($verbose >= MINVERBOSE);
        }
        elsif ($pprod_db->{section_type}->{$section} == SECTION_NAME_VALUE)
        {
            printf $log_fh "\t\t%d: Name-Value Section: %s\n", 
                __LINE__, $section;
            export_name_value_to_db($prod_file,
                                    $pprod_db,
                                    $prod_db_path,
                                    $section,
                                    $file_type);
        }
        elsif ($pprod_db->{section_type}->{$section} == SECTION_LIST)
        {
            printf $log_fh "\t\t%d: List Section: %s\n", 
                __LINE__, $section;
            export_list_to_db($prod_file,
                              $pprod_db,
                              $prod_db_path,
                              $section,
                              $file_type);
        }
        else
        {
            printf $log_fh "\t\t%d: Unknown type Section: %s\n", 
                __LINE__, $section;
        }
    }
    #
    $pprod_db->{sqlite}->{dbh}->disconnect;
    #
    return SUCCESS;
}
#
sub process_file
{
    my ($prod_file) = @_;
    #
    printf $log_fh "\n%d: Processing product File: %s\n", 
                   __LINE__, $prod_file;
    #
    my @raw_data = ();
    my %prod_db = ();
    #
    my $status = FAIL;
    if (read_file($prod_file, \@raw_data) != SUCCESS)
    {
        printf $log_fh "\t%d: ERROR Reading product file: %s\n", 
                       __LINE__, $prod_file;
    }
    elsif (process_data($prod_file, \@raw_data, \%prod_db) != SUCCESS)
    {
        printf $log_fh "\t%d: ERROR Processing product file: %s\n", 
                       __LINE__, $prod_file;
    }
    elsif (($save_to_csv == TRUE) && (export_to_csv($prod_file, \%prod_db) != SUCCESS))
    {
        printf $log_fh "\t%d: ERROR Exporting product file to CSV: %s\n", 
                       __LINE__, $prod_file;
    }
    elsif (($save_to_db == TRUE) && (export_to_db($prod_file, \%prod_db) != SUCCESS))
    {
        printf $log_fh "\t%d: ERROR Exporting product file to DB: %s\n", 
                       __LINE__, $prod_file;
    }
    else
    {
        printf $log_fh "\t%d: Success processing product file: %s\n", 
                       __LINE__, $prod_file;
        $status = SUCCESS;
    }
    #
    return $status;
}
#
######################################################################
#
my %opts;
if (getopts('?hwWv:p:l:d:RLD', \%opts) != 1)
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
    elsif ($opt eq 'R')
    {
        $rmv_prod_dir = TRUE;
    }
    elsif ($opt eq 'D')
    {
        $save_to_db = TRUE;
        $save_to_csv = FALSE;
    }
    elsif ($opt eq 'L')
    {
        $combine_lot_files = TRUE;
    }
    elsif ($opt eq 'w')
    {
        $verbose = MINVERBOSE;
    }
    elsif ($opt eq 'W')
    {
        $verbose = MIDVERBOSE;
    }
    elsif ($opt eq 'v')
    {
        if ($opts{$opt} =~ m/^[0123]$/)
        {
            $verbose = $opts{$opt};
        }
        elsif (exists($verbose_levels{$opts{$opt}}))
        {
            $verbose = $verbose_levels{$opts{$opt}};
        }
        else
        {
            printf $log_fh "\n%d: Invalid verbose level: $opts{$opt}\n", __LINE__;
            usage($cmd);
            exit 2;
        }
    }
    elsif ($opt eq 'l')
    {
        local *FH;
        $logfile = $opts{$opt};
        open(FH, '>', $logfile) or die $!;
        $log_fh = *FH;
        printf $log_fh "\n%d: Log File: %s\n", __LINE__, $logfile;
    }
    elsif ($opt eq 'p')
    {
        $product_data_dir = $opts{$opt} . '/';
        printf $log_fh "\n%d: CSV directory: %s\n", __LINE__, $product_data_dir;
    }
    elsif ($opt eq 'd')
    {
        $delimiter = $opts{$opt};
        $delimiter = "\t" if ( $delimiter =~ /^$/ );
    }
}
#
( mkpath($product_data_dir) || die $! ) unless ( -d $product_data_dir );
#
if ( -t STDIN )
{
    #
    # getting a list of files from command line.
    #
    if (scalar(@ARGV) == 0)
    {
        printf $log_fh "%d: No product files given.\n", __LINE__;
        usage($cmd);
        exit 2;
    }
    #
    foreach my $prod_file (@ARGV)
    {
        process_file($prod_file);
    }
}
else
{
    printf $log_fh "%d: Reading STDIN for list of files ...\n", __LINE__;
    while( defined(my $prod_file = <STDIN>) )
    {
        chomp($prod_file);
        process_file($prod_file);
    }
}
#
exit 0;
