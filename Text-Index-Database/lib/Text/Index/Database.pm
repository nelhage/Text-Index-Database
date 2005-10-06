package Text::Index::Database;

use 5.008004;
use strict;
use warnings;

require Exporter;
use AutoLoader qw(AUTOLOAD);

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

our @EXPORT_OK = qw( );

our @EXPORT = qw( );

our $VERSION = '0.01';


# Preloaded methods go here.

use BerkeleyDB;

use constant NEXTKEY => "_NEXTKEY";

sub new
{
	my $class = shift;
	my $self = {};
	bless($self, $class);
	if(scalar @_ % 2) {
		die("Odd number of arguments to Text::Index::Database->new");
	}
	my %args = @_;
	my $file = $args{Index} or die("No database index file specified.");
	my $stop = $args{Stop} || sub {0};
	my $skip = $args{Skip} || sub {0};
	my $wordsplit = $args{Split} || qr/[^\w]+/;
	my $ignoreCase = $args{IgnoreCase} || 0;
	$self->{IGNORE_CASE} = $ignoreCase;

	$file = "$ENV{PWD}/$file" if(substr($file, 0, 1) ne "/");
	  
	$self->{PATH} = $file;
	
	$self->_genStopFunction($stop);
	$self->{SKIP} = $self->_genFilterFunction($skip) or die("Bad ``Skip'' parameter.");

	#What constitutes a word?
	$wordsplit = qr/[^$wordsplit]/ unless ref($wordsplit) eq "Regexp";
	$self->{SPLIT} = $wordsplit;

	#open the database file
	$self->_openDB();

	$self->{FILTERS} = [["DEFAULT", sub{1}, sub{$_[0]}]];
	return $self;
}

sub _genStopFunction
{
	my ($self, $stop) = @_;
	my @stopwords;
	my $stopfun;
	if(ref($stop) eq "ARRAY") {
		@stopwords = @$stop;
	} elsif(-r $stop) {
		open(STOP, "<", $stop);
		@stopwords = <STOP>;
		close(STOP);
		chomp(@stopwords);
	} else {
		$stopfun = $self->_genFilterFunction($stop);
	}
	die("Bad ``Stop'' parameter") unless defined($stopfun);
	if(@stopwords) {
		my %stop = map{$_ => 1}@stopwords;
		$stopfun = sub {$stop{$_}};
	}
	$self->{STOP} = $stopfun;
}

sub _genFilterFunction
{
	my ($self, $fn) = @_;
	my $fun;
	if(ref($fn) eq "Regexp") {
		$fun = sub {$_ =~ $fn};
	} elsif(ref($fn) eq "CODE") {
		$fun = $fn;
	} else {
		undef $fun;
	}
	return $fun;
}

sub _openDB
{
	my $self = shift;

	my $env = new BerkeleyDB::Env
	  -Flags => DB_CREATE | DB_INIT_MPOOL | DB_INIT_CDB,
		-Home => "/tmp";
	
	$self->{ENV} = $env;

	$self->{INDEX} = new BerkeleyDB::Hash
	  -Filename => $self->{PATH},
		-Flags => DB_CREATE,
		  -Subname => "INDEX",
			-Env => $env;
	
	$self->{DOCUMENTS} = new BerkeleyDB::Hash
	  -Filename => $self->{PATH},
		-Flags => DB_CREATE,
		  -Subname => "DOCUMENTS",
			-Env => $env;
	
	$self->{DOCINDEX} = new BerkeleyDB::Hash
	  -Filename => $self->{PATH},
		-Flags => DB_CREATE,
		  -Subname => "DOCINDEX",
			-Env => $env;
	
	$self->{WORDS} = new BerkeleyDB::Hash
	  -Filename => $self->{PATH},
		-Flags => DB_CREATE,
		  -Subname => "WORDS",
			-Env => $env;

	my $max;
	if ($self->{DOCUMENTS}->db_get(NEXTKEY, $max)) {
		$self->{DOCUMENTS}->db_put(NEXTKEY, pack("I", 0));
	}
	if ($self->{WORDS}->db_get(NEXTKEY, $max)) {
		$self->{WORDS}->db_put(NEXTKEY, pack("I", 0));
	}
}

sub addFilter
{
	my ($self, $name, $condition, $action) = @_;
	if(ref($condition) eq "Regexp") {
		$condition = sub {$_ =~ $condition};
	} elsif(ref($condition) ne "CODE") {
		$condition = sub {$_ =~ /$condition/};
	}

	if(ref($action) ne "CODE") {
		die("Action parameter must be a regexp or a coderef.");
	}

	push @{$self->{FILTERS}}, [$name, $condition, $action];
}

sub removeFilter
{
	my ($self, $filter) = @_;
	@{$self->{FILTERS}} = grep {$_->[0] ne $filter} @{$self->{FILTERS}};
}

sub filterFile
{
	my ($self, $path) = @_;
	unless (-r $path) {
		die("Unable to read $path.");
	}
	my $filter;
	my @words;
	foreach $filter (@{$self->{FILTERS}}) {
		if ($filter->[1]->($path)) {
			open(FILE, "<", $path);
			local $/;
			my $text = <FILE>;
			close(FILE);

			$text = $filter->[2]->($text);
			return () unless $text;
			
			@words = split $self->{SPLIT}, $text;
			last;
		}
	}

	@words = map{lc}@words if ($self->{IGNORE_CASE});
	return @words;
}

sub extractWords
{
	my ($self, $path) = @_;
	my @words = $self->filterFile($path);
	my %words = map {$_ => 1} @words;
	@words = keys %words;
	@words = grep {!$self->{STOP}->($_)} @words;
	return @words;
}

sub indexFile
{
	my ($self, $path) = @_;
	return if $self->{SKIP}->($path);

	my @words = $self->extractWords($path);
	@words = map {$self->getWordID($_)} @words;
	my $id = $self->getDocumentID($path, @words);

	my ($word, $ids);
	
	for $word (@words) {
		if ($self->{INDEX}->db_get(pack("I",$word), $ids)) {
			$ids = "";
		}
		$ids .= pack("I", $id);

		$self->{INDEX}->db_put(pack("I",$word), $ids);
	}
}

sub getDocumentID
{
	my ($self, $path, @words) = @_;
	my $id;
	if (!$self->{DOCINDEX}->db_get($path, $id)) {
		$id = unpack("I", $id);
		$self->cleanIndex($id);
	} else {
		$self->{DOCUMENTS}->db_get(NEXTKEY, $id);
		$id = unpack("I", $id);
		$self->{DOCUMENTS}->db_put(NEXTKEY, pack("I",$id+1));
		$self->{DOCINDEX}->db_put($path, pack("I", $id));
	}
	$self->{DOCUMENTS}->db_put(pack("I", $id), "$path\0" .
								 pack("I*", @words));
	return $id;
}

sub getWordID
{
	my ($self, $word) = @_;
	my $id;
	if (!$self->{WORDS}->db_get($word, $id)) {
		return unpack("I",$id);
	} else {
		$self->{WORDS}->db_get(NEXTKEY, $id);
		$id = unpack("I", $id);
		$self->{WORDS}->db_put(NEXTKEY, pack("I", $id+1));
		$self->{WORDS}->db_put($word, pack("I",$id));
		return $id;
	}
}

sub cleanIndex
{
	my ($self, $id) = @_;
	my $words;
	my @words;
	my @docs;
	my $docs;
	$self->{DOCUMENTS}->db_get(pack("I",$id), $words);
	$words =~ s/^.*?\0//;
	@words = unpack("I*", $words);
	for my $word (@words) {
		$self->{INDEX}->db_get(pack("I",$word), $docs) and next;
		@docs = grep{$_ != $id} unpack("I*", $docs);
		$self->{INDEX}->db_put(pack("I",$word), pack("I*", @docs));
	}
}

sub allDocuments
{
	my $self = shift;
	my @documents;
	my $cursor = $self->{DOCUMENTS}->db_cursor();

	my ($key, $value);

	until ($cursor->c_get($key, $value, DB_NEXT) == DB_NOTFOUND) {
		push @documents, $value unless $key eq NEXTKEY;
	}
	return @documents;
}

sub search
{
	my ($self, $query) = @_;
	my $docs;
	my @docs;
	my $document;
	my $word;

	my ($include, $exclude, $iphrases, $ephrases) = $self->_parseQuery($query);
	my @match;

	my %matches = ();
	for $word (@$include) {
		next if $self->{STOP}->($word);
		$word = $self->getWordID($word);
		return if $self->{INDEX}->db_get(pack("I", $word), $docs);
		@docs = unpack("I*", $docs);
		if (!scalar keys %matches) {
			$matches{$_} = 1 foreach (@docs);
		} else {
			%matches = _union(\%matches, @docs);
		}
	}

	for $word (@$exclude) {
		next if $self->{STOP}->($word);
		$word = $self->getWordID($word);
		$self->{INDEX}->db_get(pack("I", $word), $docs) and next;
		delete $matches{$_} foreach (unpack("I*", $docs));
	}

	return unless scalar keys %matches;
	@match = map{$self->{DOCUMENTS}->db_get(pack("I",$_), $document);
				 substr($document, 0, index($document, "\0"))} keys %matches;
	@match = $self->_matchPhrases(\@match, $iphrases, $ephrases);

	return @match;
}

sub _union
{
	my ($hash, @docs) = @_;
	my %docs;
	return map {$_ => 1} (grep {$hash->{$_}} @docs);
}

sub _parseQuery
{
	my ($self, $query) = @_;
	$query = lc $query if $self->{IGNORE_CASE};
	my @include;
	my @exclude;
	my @iphrases;
	my @ephrases;
	my $include;

	my $word;
	my $words;

	while ($query =~ /([-+]?)"/) {
		$words .= " $`";
		my $inc = ($1 ne "-");
		$query = $';
		if ($query =~ /"/) {
			if ($inc) {
				push @iphrases, $`;
				$words .= " $`";
			} else {
				push @ephrases, $`;
			}
			$query = $';
		} else {
			$query =~ s/^.*"//;
		}
	}
	$words .= $query;
	my @words = split /[^a-zA-Z0-9_+-]+/, $words;
	
	for $word (@words) {
		if (substr($word, 0, 1) eq "+" ||
			  substr($word, 0, 1) eq "-") {
			$include = $word =~ /^\+/;
			$word = substr($word, 1);
		} else {
			$include = 1;
		}
		if ($include) {
			push @include, $word;
		} else {
			push @exclude, $word;
		}
	}

	return (\@include, \@exclude, \@iphrases, \@ephrases);
}

sub _matchPhrases
{
	my ($self, $match, $inc, $exc) = @_;
	my $phrase;
	foreach $phrase (@$inc) {
		@$match = grep {$self->_findPhrase($_, $phrase)} @$match;
	}

	foreach $phrase (@$exc) {
		@$match = grep {!$self->_findPhrase($_, $phrase)} @$match;
	}

	return @$match;
}

sub _findPhrase
{
	my ($self, $file, $phrase) = @_;
	my @words;
	eval {
		@words = $self->filterFile($file) or return 0;
	};
	return 0 if index($@, "Unable to read") == 0;
	$phrase = join("\0",split $self->{SPLIT}, $phrase);
	return index(join("\0",@words), $phrase) != -1;
}

# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__

=head1 NAME

Text::Index::Database - Perl extension for full-text indexing of files

=head1 SYNOPSIS

  use Text::Index::Database;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Text::Index::Database, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Nelson Elhage, E<lt>nelhage@mit.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2005 by Nelson Elhage

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
