package MarcExport;

use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

use Exporter;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use URI::Escape;
use Encode;

$VERSION = 1.00;
@ISA = qw(Exporter);

@EXPORT = qw(
		generate_MARC
		marc_text
            );

sub generate_MARC
{
    my ($DEBUG, $savelog, $type, $id, $originalmarc, $pids, $PID, $barcodePID, $serialhash, $marc, $date, $editor, $source, $callnumber, $label, $sortkey, $barcode) = @_;
    my (%originalmarc, %serials, %originalmarc, %PIDs, %text);

    %serials = %{$serialhash} if (%$serialhash);
    $originalmarc{$id} = $originalmarc;
    $PIDs{$id} = $pids if ($pids);
    print $savelog "[M1] DEBUG PID $PID\n" if ($DEBUG eq 'savelog');
    print "D $DEBUG $savelog\n";

    if ($id)
    {
        $marc=~s/\r|\n//g;
	$marc = decode('iso-8859-1', $marc);

	my $tmpmarc = $marc;
	my $marcshow;
	while ($tmpmarc=~s/(^.+?<\/\S+?>)//)
	{
	   $marcshow.= "$1\r\n";
	}
	$marc = $marcshow;
	$marc=~s/\s+/ /g;

	if ($marc)
	{
            $marc =~ s/\n//sgo;
            $marc =~ s/^<\?xml.+\?\s*>//go;
            $marc =~ s/>\s+</></go;
            $marc =~ s/\p{Cc}//go;
            $marc=~s/(<\/datafield>)/$1\n/g;
	};

	# Delete 902 fields
	if ($marc=~/902\".+?\"a\">(\S+?)</)
	{
	    $PID = $1;
	    $PID=~s/\r|\n//g;
	}

	$marcrecord = MARC::Record->new_from_xml($marc);

        # 852 field 
        my $locmarc = MARC::Field->new('852','','','a' => 'IISG', 'b' => 'IISG', 'c' => 'IISG', 'j' => $label, 'p' => $barcode);
        $marcrecord->insert_fields_ordered($locmarc);

        # 856|u
	if ($marc!~/handle\.net/i && $barcode=~/^3005/)
        {
            my $umarc = MARC::Field->new('856','','','u' => 'http://hdl.handle.net/10622/'.$barcode);
            $marcrecord->insert_fields_ordered($umarc);
        }

	# Storing barcodes
	if ($barcode=~/^3005/ && !$barcodePID)
	{
	    my $command = "/openils/applications/PID-webservice/examples/perl/pid.realtime.pl -i $id -b $barcode";
	    print $savelog "[DEBUG barcodePID] $command\n" if ($DEBUG eq 'savelog');
	    $pidtmp = `$command`;
	    $barcodepids{$id} = "10622/$barcode";
	    $barcodePID = $barcode;
	}

	if ($serials{$label})
	{
	   my $serialrecord = $serials{$label};
           my $pidfield = $serialrecord->field( "866" );
           if ($pidfield)
           {
              $marcrecord->insert_fields_ordered($pidfield);
	   };
	}

	# make new PID for images without barcodes
	if (!$PID)
	{
	    my $command = "/openils/applications/PID-webservice/examples/perl/pid.realtime.pl -i $id";
	    my $pidtmp = `$command`;
	    print $savelog "[DEBUG barcode] $command\n" if ($DEBUG eq 'savelog');
	    if ($pidtmp=~/^(\d+)\s+\=>\s+(\S+)/)
	    {
		$PID = $2;
                my $pidfield = MARC::Field->new('902','','','a' => $PID);
                $marcrecord->insert_fields_ordered($pidfield);
		$realtimePIDS{$id} = $PID;
	    }
	}

        # 902|a field
        if ($PID=~/\d+/ && $PID!~/\/3005\d+/i)
        {
            my $pidfield = MARC::Field->new('902','','','a' => $PID);
            $marcrecord->insert_fields_ordered($pidfield);
        }

	if ($marcpid)
	{
	    my $pidfield = $marcrecord->field( "902" );
	    if ($pidfield)
	    {
	        $marcrecord->delete_field( $pidfield );
	        $marcrecord->insert_fields_ordered($pidfield);
	    }
	};

	my $thismarc = marc_text($type, $marcrecord, $id, $PID);

	# New record
	unless ($PIDs{$id})
	{
	   push(@resultset, $id);
	   $counter{$id} = 1;
           my $pidfield = $marcrecord->field( "852" );
           if ($pidfield)
           {
                $marcrecord->delete_field( $pidfield );
		$pidfield->add_subfields( 't' => $counter{$id});
                $marcrecord->insert_fields_ordered($pidfield);
           }

	   $text{$id} = $thismarc;
	   $originalmarc{$id} = $marcrecord;
	}
	else
	# Another holding record
	{
	   print "Another ID $counter{$id}\n" if ($DEBUG);

	   $counter{$id}++;
           my $pidfield = $marcrecord->field( "852" );
	   $originalmarc = $originalmarc{$id};
           if ($pidfield)
           {
	      $pidfield->add_subfields( 't' => $counter{$id});
              $originalmarc->insert_fields_ordered($pidfield);
           }

           my $serialfield = $marcrecord->field( "866" );
           if ($serialfield)
           {
              $originalmarc->insert_fields_after($pidfield, $serialfield);
           }

	   $originalmarc{$id} = $originalmarc;
	   $text{$id} = marc_text($type, $originalmarc{$id}, $id, $PIDs{$id});
	};

	$PIDs{$id} = $PID unless ($PIDs{$id});
	$originalmarc{$id} = $marcrecord unless ($originalmarc{$id});
    }

    return ($originalmarc{$id}, $text{$id}, $PIDs{$id}, $barcodepids{$id});
}

sub marc_text
{
   my ($type, $marcrecord, $id, $PID, $istext, $DEBUG) = @_;
   my $thismarc;

   if ($marcrecord)
   {
        unless ($istext)
        {
            $thismarc = $marcrecord->as_xml_record();
        }
        else
        {
            $thismarc = $marcrecord;
        }

        $thismarc=~s/<\?xml.+?>//g;

        # date 2011-10-03 11:33:12.067247+02
        $pubdate = "2011-12-03T11:14:49.086Z";
        if ($date=~/^(\d+\-\d+\-\d+)\s+(\d+\:\d+\:\d+)/)
        {
            $pubdate = "$1T$2\.000Z";
        }

        my $setSpec = "$id";
        my $setSpectmp = "$setSpec.$PID";
        $setSpectmp = $sqlquery;
        $marccount++;

	if ($type=~/oai/i)
	{
           $setSpectmp = "$token $from_date-$until_date #$marccount";
           $header="\n<header><identifier>oai:socialhistory.org:$PID</identifier><datestamp>$pubdate</datestamp><setSpec>$setSpec</setSpec><setSpec>$setSpectmp</setSpec></header>\n";
	};

        $thismarc=~s/<datafield/<marc\:datafield/gsxi;
        $thismarc=~s/<\/datafield/<\/marc\:datafield/gsxi;
        $thismarc=~s/<subfield/<marc\:subfield/gsxi;
        $thismarc=~s/<\/subfield/<\/marc\:subfield/gsxi;
        $thismarc=~s/<controlfield/<marc\:controlfield/gsxi;
        $thismarc=~s/<\/controlfield/<\/marc\:controlfield/gsxi;
        $thismarc=~s/<leader/<marc\:leader/gsxi;
        $thismarc=~s/<\/leader/<\/marc\:leader/gsxi;

        unless ($istext)
        {
	   if ($type=~/oai/i)
	   {
                $thismarc=~s/<(record.+?)>\s*/<record>$header<metadata>\n<marc\:record xmlns\:marc\=\"http\:\/\/www.loc.gov\/MARC21\/slim\">/gsxi;
		$thismarc=~s/<\/record>/<\/marc\:record>\n<\/metadata>\n<\/record>/g;
	   }
	   else
	   {
		$thismarc=~s/<(record.+?)>\s*/<marc\:record>\n/gsxi;
		$thismarc=~s/<\/record>/<\/marc\:record>/g;
	   };

#           $thismarc=~s/<\/record>/<\/marc\:record>\n<\/metadata>\n<\/record>/g;
        }
        else
        {
           $thismarc=~s/<(record.+?)>/<marc\:record xmlns\:marc\=\"http\:\/\/www.loc.gov\/MARC21\/slim\">/gsxi;
           $thismarc=~s/<\/record>/<\/marc\:record>/g;
        };

	$thismarc=~s/(code\=\"\w+\"\>)(\<\/marc\:subfield\>)/$1Unknown$2/g;
   }

   return $thismarc;
}

