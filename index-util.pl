#!/usr/bin/env perl
use strict;
use warnings;

use Text::Index::Database;
use Data::Dumper;
use File::Find;

use constant CONFIG_DIR => "$ENV{HOME}/.indexes";
use constant CONFIG_FILE => CONFIG_DIR . "/indexes";

mkdir(CONFIG_DIR) unless -d CONFIG_DIR;

my $indexes;
  
unless (-f CONFIG_FILE) {
	$indexes = {};
} else {
	my $cfg;
	open(CFG, "<", CONFIG_FILE);
	local $/;
	$cfg = <CFG>;
	close(CFG);
	eval $cfg;
}

sub usage {
	print "Usage: index-util <command> <index> <args>\n";
	print "Run index-util help <command> for more detailed help.\n";
	print "\ncommands:\n";
	print "\n\tFIXME\n";
	exit(-1);
}

my %commands = (
	add => \&createIndex,
	create => \&createIndex,
	list => \&listIndexes,
	ls => \&listIndexes,
	update => \&updateIndex,
	reload => \&updateIndex,
	scan => \&updateIndex,
	up => \&updateIndex,
	delete => \&deleteIndex,
	remove => \&deleteIndex,
	del => \&deleteIndex,
	rm => \&deleteIndex,
	search => \&searchIndex,
	find => \&searchIndex
   );

my $command = shift || usage();
my $index = shift;
$index = lc($index);

my $method = $commands{$command};

die("Unknown command $command") unless defined($method);

$method->($index, @ARGV);

open(CFG, ">", CONFIG_FILE);
print CFG Data::Dumper->Dump([$indexes], ["indexes"]);
close(CFG);

sub openDB {
	my $index = shift;
	exists $indexes->{$index} or die("Index `$index' does not exist");
	$index eq "all" and die("Invalid index: `$index'");
	my $db = Text::Index::Database->new(Index => CONFIG_DIR . "/" . "$index" . ".db",
										Stop => sub{length $_[0] <= 3},
										IgnoreCase => 1);
	return $db;
}

sub createIndex {
	my $index = shift;
	my $path = shift || die("Usage: create <path>");
	-d $path or die("Path `$path' does not exist");
	if (exists($indexes->{$index})) {
		die("Error: index `$index' already exists");
	}
	my %index = (
		ROOT => $path,
		UPDATED => 0,
	);
	$indexes->{$index} = \%index;
}

sub listIndexes {
	my ($name, $val);
	while (($name, $val) = each %$indexes) {
		print "$name\t\t$val->{ROOT}\n";
	}
}

sub updateIndex {
	my $index = shift;
	if ($index eq "all") {
		foreach $index (keys %$indexes) {
			updateIndex($index);
		}
		return;
	}
	my %index = %{$indexes->{$index}};
	my $force = shift;
	$force = defined($force) && $force eq "--force";
	
	my $db = openDB($index);
	my @docs = $db->allDocuments();
	foreach my $file (@docs) {
		unless (-e $file) {
			$db->removeFile($file)
		}
	}

	find(sub {
			 return unless -f;
			 my @stat = stat($File::Find::name);
			 my $mtime = $stat[9];
			 return if $mtime <= $index{UPDATED} && !$force;
			 return if /~$/;
			 $db->indexFile($File::Find::name);
		 }, $index{ROOT});
	$indexes->{$index}->{UPDATED} = time;
}

sub deleteIndex {
	my $index = shift;
	exists $indexes->{$index} or die("Index `$index' does not exist");
	unlink(CONFIG_DIR . "/" . $index . ".db");
	delete $indexes->{$index};
	print "Index `$index' destroyed.\n";
}

sub searchIndex {
	my ($index, @args) = @_;
	my $db = openDB($index);
	my $search = join(" ", @args);
	my @results = $db->search($search);
	print join("\n", @results);
	print "\n" if scalar @results;
}
