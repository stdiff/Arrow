#!/usr/bin/perl 
# >Last Modified on Thu, 17 Dec 2015< 
use strict;
use warnings;
use utf8;
use Data::Dumper qw(Dumper);
use Pod::Markdown;

my $pod = "./Arrow/DataFrame.pm";

open my $fh, '<', $pod;
my $pod_on = 0;
my $pod_string = "";
while (<$fh>) {
  $pod_string .= $_ if $pod_on;
  $pod_on = 1 if /^=pod/;
  $pod_on = 0 if /^=cut/;
}
close $fh;

my $parser = Pod::Markdown->new();
my $markdown;
$parser->output_string( \$markdown );
$parser->parse_string_document($pod_string);

$markdown =~ s/\\`/`/g;
$markdown =~ s/\\_/_/g;
$markdown =~ s/\\\[/[/g;
$markdown =~ s/\\\]/]/g;
$markdown =~ s/\\\\&/\\&/g;

open my $md, '>', 'pod-DataFrame.md';
print $md $markdown;
close $md;
exit;
