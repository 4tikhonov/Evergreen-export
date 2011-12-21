#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/libs";

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DB_File;
use MarcExport;

use Convert::Cyrillic;
use DBI;
$| = 1;

my %dbconfig = loadconfig("$Bin/db.config");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

use Getopt::Std;
%options=();
getopts("dp:s:f:l:OLi:SP:r",\%options);

$type = "LOC";
$DEBUG++ if ($options{d});
$page = $options{p} if ($options{p});
$start = $options{s} if ($options{s});
$finish = $options{f} if ($options{f});
$limit = $options{l} if ($options{l});
$type = "OAI" if ($options{O});
$type = "LOC" if ($options{L}); 
$uID = $options{i} if ($options{i});
$pages = $options{P} if ($options{P});
$shortDEBUG++ if ($options{S});
$regenerate++ if ($options{r});

#OAI-PMH:
#- verbs: Identify, ListSets?, ListMetadataPrefix?, GetRecord?, ListIdentifiers? and ListRecords?
#- support for the -from and -until parameters
#- supported metadataPrefix = marcxml

$uri = $ENV{REQUEST_URI};
use URI::Escape;
use Encode; # 'decode_utf8';
my $query_string = $uri; # $ENV{QUERY_STRING};

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$limit = 1000 unless ($limit);
if (!$start && !$pages)
{
   $pages = get_pages($dbh, $limit); # print "$pages x $limit\n"; exit(0);
};

if ($year)
{
   $year+=1900;
   $mon++;
   $expirationDate = sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", $year, $mon, $mday, 23, 59, $sec);
   $responseDate = sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", $year, $mon, $mday, $hour, $min, $sec);
};

$dbpid = "/openils/applications/pids/pids.db";
$barpid = "/openils/applications/pids/pids.barcode.db";
$EXT_PIDS = 1 if (-e $dbpid);
$EXT_PIDS = 0 if ($regenerate);

if ($EXT_PIDS)
{
   tie %realtimepids, 'DB_File', $dbpid;
   tie %barcodepids, 'DB_File', $barpid;
}

use CGI;
use Encode 'decode_utf8';
use utf8;

# import the marc8_to_utf8 function
use MARC::Charset 'marc8_to_utf8';
binmode(STDOUT, 'utf8');

if ($query=~/[a-zA-Z]/)
{
   $Src = "VOL";
   $Dst = "utf8";
}
else
{
   $Src = 'utf8';
   $Dst = 'VOL';
   $xquery = Convert::Cyrillic::cstocs ($Src, $Dst, $query);
   $query = $xquery if ($xquery);
}; 

show_header_oai() if ($type=~/oai/i);
show_header_loc() if ($type=~/loc/i);
for ($i=0; $i<$pages; $i++)
{
    $page = $i;
    show_biblio($dbh, $page, $limit, $uID);
};
show_footer_oai() if ($type=~/oai/i);
show_footer_loc() if ($type=~/loc/i);

if ($EXT_PIDS)
{
   untie %realtimepids;
   untie %barcodepids;
};

sub show_biblio
{
    my ($dbh, $page, $limit, $uID, $ids) = @_;
    my (@resultset, %ready, $marcfinal, @items, %text, %originalmarc, %PIDs, %mainmarc);
 
    $ids=~s/\,\s+$//g;
    my $sqlquery = "select b.id, b.marc, b.edit_date, b.editor, b.source, c.call_number, n.label, n.label_sortkey, c.barcode from asset.call_number as n, asset.copy as c, biblio.record_entry as b where n.id=c.call_number and n.record=b.id";
    my $offset;
    $offset = $limit * $page;

    $sqlquery.=" and b.id in ($ids)" if ($ids);
    $sqlquery.=" and b.id = $uID" if ($uID);
    $sqlquery.=" and b.edit_date >= '$from_date'" if ($from_date);
    $sqlquery.=" and b.edit_date < '$until_date'" if ($until_date);  
    $sqlquery.=" order by b.id desc" unless ($found);
    $sqlquery.=" limit $limit";
    $sqlquery.=" offset $offset" if ($offset);
    print "$sqlquery\n\n" if ($UID && $DEBUG); # if ($DEBUG2);
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($id, $marc, $date, $editor, $source, $callnumber, $label, $sortkey, $barcode) = $sth->fetchrow_array())
    {
	$barcode=~s/10622\///g;
	$sortkey=~s/\_/\//g;
	$item = "$id%%$marc%%$date%%$editor%%$source%%$callnumber%%$label%%$sortkey%%$barcode";
	push(@items, $item);
	#print "[DEBUG] $item\n";
	$ids.="$id, " unless ($known{$id});
	$known{$id}++;
    }

    $ids=~s/\,\s*$//g;
    my %serials = extract_serials($dbh, $ids);

    foreach $item (@items)
    {
	my ($id, $marc, $date, $editor, $source, $callnumber, $label, $sortkey, $barcode) = split(/\%\%/, $item);
        $PID = $realtimepids{$id} if ($realtimepids{$id});
        $BarcodePID = $barcodepids{$id} if ($barcodepids{$id});
	print "[DEBUGMAIN TCN #$id] EXT=$EXT_PIDS PIDS=$PID Barcode=$BarcodePID\n" if ($shortDEBUG);

	($original, $showmarc, $pids, $barcodepids) = generate_MARC($type, $id, $originalmarc{$id}, $PIDs{$id}, $PID, $BarcodePID, $serials{$id}, $marc, $date, $editor, $source, $callnumber, $label, $sortkey, $barcode);
	$originalmarc{$id} = $original;
	$text{$id} = $showmarc;
	$mainmarc{$id} = $showmarc;
	$PIDs{$id} = $pids;

	unless ($realtimepids{$id})
	{
	   $realtimepids{$id} = $pids;
	   print "[DEBUG save #$id] $EXT_PIDS $realtimepids{$id} $BarcodePID\n" if ($shortDEBUG);
	}
	if (!$barcodepids{$id} && $barcodepids)
	{
	   $barcodepids{$id} = $barcodepids;
	}

	unless ($ready{$id})
	{
	    push(@resultset, $id);
	    $ready{$id}++;
	}
    }

    $ids=~s/\,\s*$//g;

    foreach $id (@resultset)
    {
        $marcfinal.= $text{$id}; 
    }
    print "$marcfinal" unless ($shortDEBUG);

    $found = $marccount unless ($found);
    $page++;
    $page = '0' unless ($page);
    $nexttoken = "$control:$page:$limit:$from_date:$until_date:$keywords";

    return;
}


sub extract_serials
{
    my ($dbh, $ids, $DEBUG) = @_;
    my %serials;

    return unless ($ids);
    $sqlquery = "select record, marc from serial.record_entry where record in ($ids) order by record desc";
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($id, $marc) = $sth->fetchrow_array())
    {
	my $marcrecord = MARC::Record->new_from_xml($marc);	
	my $label = $marcrecord->subfield('852',"j");
	$serials{$id}{$label} = $marcrecord;
	print "Put $marcrecord => $id *$label* $serials{$id}{$label}\n" if ($DEBUG);

    };

    return %serials;
}

sub show_next_token
{
   if ($found)
   {
print << "EOF";
<resumptionToken cursor="$page" completeListSize="$found" expirationDate="$expirationDate">$nexttoken</resumptionToken>
EOF
   }
}

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
	my $str = $_;
	$str=~s/\r|\n//g;
	my ($name, $value) = split(/\s*\=\s*/, $str);
	$config{$name} = $value;
    }
    close(conf);

    return %config;
}

sub show_header_loc
{
print <<"EOF";
<?xml version="1.0" encoding="UTF-8"?>
<marc:collection xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
EOF
}

sub show_footer_loc
{
print <<"EOF";
</marc:collection>
EOF
}

sub show_header_oai
{
print <<"EOF";
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"><ResponseDate>$responseDate</ResponseDate><Request verb="ListRecords" metadataPrefix="marcxml"/><ListRecords>
EOF

   return;
}

sub show_footer_oai
{
print << "EOF";
</ListRecords>
</OAI-PMH>
EOF

    return;
}

sub get_pages
{
    my ($dbh, $limit, $DEBUG) = @_; 

    $sqlquery = "select count(*) from biblio.record_entry";
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    my ($amount) = $sth->fetchrow_array();
    my $pages = int($amount / $limit) + 1;

    return $pages;
}
