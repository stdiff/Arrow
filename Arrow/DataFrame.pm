package Arrow::DataFrame;
# >Last Modified on Thu, 11 Feb 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use Encode;
use List::MoreUtils qw(uniq);
use Text::CSV;
# use Text::CSV::Encoded;
use Math::MatrixReal;

use Arrow::GroupBy;

######################################### Constructors
sub new {
  my $class = shift;
  my %df = @_;
  $df{rows} ||= [];

  if (!defined($df{names})) {
    if (defined($df{rows}->[0])) {
      my $p = @{$df{rows}->[0]};
      $df{names} = [map { "X$_" } 1..$p];
    } else {
      $df{names} = []; 
    }
  }

  ## display options
  $df{width} = 8;
  $df{separator} = '|';

  return bless \%df, $class;
}


sub hash_to_dataframe {
  my $class = shift;
  my $hashref = shift;
  my $names = shift;
  $names ||= [sort keys %$hashref];
  my $n = @{$hashref->{$names->[0]}}-1;

  my @rows;
  foreach my $i (0..$n) {
    push(@rows,[map { $hashref->{$_}->[$i] } @$names]);
  }
  return Arrow::DataFrame->new(rows=>\@rows,names=>$names);
}


sub read_csv {
  my $class = shift;
  my $file = shift; # CSV file 
  my $pref = shift; # preference for Text::CSV
  my $df; # data frame we create

  $pref->{binary} ||= 1;
  $pref->{sep_char} ||= ',';
  $pref->{blank_is_undef} ||= 1;
  # $pref->{encoding_in} ||= 'utf8'; # for Text::CSV::Encoded

  my $header = 1; # whether the file has a header. Default: header = T in R
  if (defined($pref->{header})) {
    $header = $pref->{header};
    delete $pref->{header};
  }

  # my $csv = Text::CSV::Encoded->new($pref); # for Text::CSV::Encoded
  my $csv = Text::CSV->new($pref);

  # if (open my $fh, '<', $file) { # for Text::CSV::Encoded
  if (open my $fh, "<:encoding(utf8)", $file) {

    if ($header) {
      ### header=T
      my $names = $csv->getline($fh);
      $df = Arrow::DataFrame->new(names=>$names);
    } else {
      ### header=F
      ### We need to read the first line to find out the number of columns
      my $row0 = $csv->getline($fh); 
      my $p = $#$row0;
      $df = Arrow::DataFrame->new(rows=>[$row0], names=>[map { "X$_" } 0..$p])
    }

    while (my $row = $csv->getline($fh)) {
      $df = $df->rbind($row);
    }
    close $fh;
  } else {

  }
  return $df;
}


sub read_sql{
  my $class = shift;
  my $dbh = shift; # an instance of DBI
  my $sql = shift; # sql command
  my $var = shift; # variables for the place holder

  my $sth = $dbh->prepare($sql);
  $sth->execute(@$var);
  my $names = $sth->{NAME};
  return Arrow::DataFrame->new(names=>$names,rows=>$sth->fetchall_arrayref());
}


sub matrix_to_dataframe {
  my $class = shift;
  my $matrix = shift;
  my $names = shift;
  my ($n,$p) = $matrix->dim();
  $p--;

  # initialize names of columns if the size of the array of names is wrong.
  if ($#$names != $p) {
    croak "The number of columns does not agree with the number of given names.";
    @$names = map { "X$_" } 0..$p;
  }

  my $df = Arrow::DataFrame->new(names=>$names);

  foreach my $i (1..$n) {
    my $row = $matrix->row($i);
    my @array = $row->as_list();
    $df->rbind(\@array);
  }
  return $df;
}


########################################### getter and setters
sub separator {
  if ($_[1]) {
    return $_[0]->{separator} = $_[1];
  } else {
    return $_[0]->{separator};    
  }
}

sub width {
  if ($_[1]) {
    return $_[0]->{width} = $_[1];
  } else {
    return $_[0]->{width};    
  }
}

sub names{
  if ($_[1]) {
    return $_[0]->{names} = $_[1];
  } else {
    return $_[0]->{names};
  }
}

sub set_name {
  my $self = shift;
  my $col = shift;  # a string
  my $index = shift; # an integer
  $self->{names}->[$index] = $col;

  return $self->{names};
}


########################################################### write a data frame
sub write_csv {
  my $self = shift;
  my $file = shift;

  # use Text::CSV::Encoded;
  # my $csv = Text::CSV::Encoded->new({encoding_out => 'utf8'});
  # open my $fh, ">", "$file";

  my $csv = Text::CSV->new();
  open my $fh, ">:encoding(utf8)", "$file";

  $csv->print($fh,$self->names); # header
  print $fh "\n";
  foreach my $row (@{$self->{rows}}) {
    $csv->print($fh,$row);
    print $fh "\n";
  }
  close $fh;
}


### - write_sql($dbh,$table,$record) ##### does not work!!!!
### $record 
### [ [COL1,COL2,COL3,...],['TEXT','INTEGER','REAL',...] ]
### Rmk: This method delete the table at first!
sub write_sql{
  die; 
  my $self = shift;
  my $dbh = shift;
  my $table = shift;
  my $record = shift;
  my $cols = $record->[0];
  my $type = $record->[1];

  my $sql = "DROP TABLE IF EXISTS $table";
  $dbh->do($sql);

  $sql = join ", ", map { "$cols->[$_] $type->[$_]"  } 0..$#$cols;
  $sql = "CREATE TABLE $table ($sql)"; # DESC TEXT,
  $dbh->do($sql);

  my $names = $self->names;
  my $n = $self->nrow()-1;

  my $placeholder = join ", ", qw(?) x @$cols;
  my $cnames = join ", ", @$cols;
  $sql = "INSERT INTO $table($cnames) VALUES($placeholder)";
  print $sql;
  my $sth = $dbh->prepare($sql);

  my $col_names = $self->names_to_cols($cols);
  foreach my $i (0..$n) {
    my $row = $self->rows();
    my @vals = @$row[@$col_names];
    print $i;
    $sth->execute(@vals);
  }
}


######################################################### read a data frame

sub nrow {
  my $self = shift;
  my $n = @{$self->{rows}};
  return $n;
}

sub ncol {
  my $self = shift;
  my $p = @{$self->names()};
  return $p;
}

sub show {
  my $self = shift;
  my %pref = @_;
  my $n = $self->nrow;
  ### initialisation of the preference
  $pref{separator} ||= $self->separator;
  $pref{width} = $self->width if not $pref{width};
  $pref{from} ||= 0;
  $pref{to} = $n-1 unless defined($pref{to});

  my @names = @{$self->names()};
  my @range = (0,$n-1); # initialisation

  my $head = $pref{head};
  if ($head) {
    if ($head > 0 && $head < $n) {
      $range[1] = $head-1;
    } elsif ($head < 0 && $n+$head > 0) {
      my $m = $n + $head;
      $range[0] = $m;
    }
  } else {
    $range[0] = $pref{from};
    if ($pref{from} <= $pref{to}) { # ignore 'to<from' case
      $range[1] = $pref{to};
    }
  }

  ### prepare for dummy sequence of spaces
  my $empty = "";
  foreach (1..$pref{width}) { $empty .= " "; }

  @names = map {
    if ($_ eq "") {
      substr("NA$empty",0,$pref{width})
    } else {
      substr("$_$empty",0,$pref{width})
    }
  } @names;

  my $sep = $pref{separator};
  print "    $sep ";
  print join " $sep ", @names;
  print "\n";

  foreach my $i ($range[0]..$range[1]) {
    my @row = map {
      if (defined $_) {
	if ($_ eq "") {
	  substr("NA$empty",0,$pref{width})
	} else {
	  substr("$_$empty",0,$pref{width})
	}
      } else {
	substr("NA$empty",0,$pref{width})	
      }
    } @{$self->{rows}->[$i]};
    printf ("%03s $sep ",$i);
    print join " $sep ", @row;
    print "\n";
  }
  return $self;
}


sub head {
  my $self = shift;
  my $n = shift;
  $n = 5 if not defined($n);
  $self->show(head=>$n);
}


sub tail {
  my $self = shift;
  my $n = shift;
  $n = 5 if not defined($n);
  $self->show(head=>-$n);
}


sub names_to_cols {
  my $self = shift;
  my $given_names = shift;
  if (not defined $given_names) { return undef }

  my $names = $self->names;
  my $p = @$names - 1;

  # create a 'funciton' sending a name to its index 
  my $name_to_column;
  foreach my $j (0..$p) {
    $name_to_column->{$names->[$j]} = $j;
  }

  if (ref $given_names) {
    return [map {
      if (/^-?\d+$/) { $_ } else { $name_to_column->{$_} }
    } @$given_names];
  } else {
    if ($given_names =~ /^-?\d+$/) {
      return $given_names;
    } else {
      my $col_name = $name_to_column->{$given_names};
      return $col_name;
    }
  }
}


sub cols_to_names {
  my $self = shift;
  my $cols = shift;
  my $names = $self->names();

  if (ref $cols) {
    return [(@$names[@$cols])];
  } else {
    return $names->[$cols];
  }
}


sub rows{
  my $self = shift;
  my $rows = shift;
  my $range = $self->_give_specified_range($rows,$self->nrow()-1);

  my @subset;
  foreach my $i (@$range) {
    my @row = @{$self->{rows}->[$i]};
    push(@subset,\@row);
  }

  if (ref $rows) {
    return \@subset;
  } else {
    return $subset[0];
  }
}


sub rows_hash {
  my $self = shift;
  my $rows = shift;

  my $range = $self->_give_specified_range($rows,$self->nrow()-1);
  my $names = $self->names;
  my $p = $self->ncol()-1;

  my @subset;
  foreach my $i (@$range) {
    my @row = @{$self->{rows}->[$i]};
    my $row_hash;
    foreach my $j (0..$p) {
      $row_hash->{ $names->[$j] } = $row[$j];
    }
    push (@subset,$row_hash);
  }

  if (ref $rows) {
    return \@subset;
  } else {
    return $subset[0];
  }
}


sub cols {
  my $self = shift;
  my $cols = shift;
  my $n = $self->nrow()-1;

  my $columns = $self->names_to_cols($cols);
  my $range = $self->_give_specified_range($columns,$n);

  my @cols;
  foreach my $j (@$range) {
    my @vec;
    foreach my $i (0..$n) {
      push(@vec,$self->{rows}->[$i]->[$j]);
    }
    push(@cols,\@vec);
  }

  if (ref $cols) {
    return \@cols;
  } else {
    return $cols[0];
  }
}


sub at {
  my $self = shift;
  my @result;

  foreach my $coord (@_) {
    my ($i,$j) = @$coord;
    push(@result, $self->{rows}[$i][$j]);
  }
  return \@result;
}

######################################################## tweeking a data frame
sub add_name{
  my $self = shift;
  my $name = shift;

  if (defined $self->names_to_cols($name)) {
    # if the given name is one of the names of columns, then return undef.
    return undef;
  } else {
    my $k = @{$self->{names}}; # the col number of the name which will be added
    push (@{$self->{names}},$name);
    return $k;
  }
}


sub del_cols{
  my $self = shift;
  my $cols = shift; # array of column numbers 

  my $col_numbers = $self->names_to_cols($cols); ### names to column number
  if (ref $cols) {
    @$col_numbers = $self->_make_index_positive(@$col_numbers,$self->ncol);
  } else {
    $col_numbers = [$col_numbers];
  }

  my @removed;
  my $n = $self->nrow; $n--;
  foreach my $j (@$col_numbers) {
    splice (@{$self->{names}},$j,1); 
    # reverse sort is required for the above line

    my @removed_col;
    foreach my $i (0..$n) {
      $removed_col[$i] = splice(@{$self->{rows}->[$i]},$j,1);
    }
    push(@removed,\@removed_col);
  }

  if (ref $cols) {
    return \@removed;
  } else {
    return $removed[0];
  }
}


sub del_rows {
  my $self = shift;
  my $rows = shift;

  my @row_numbers;
  if (ref $rows) {
    @row_numbers = $self->_make_index_positive(@$rows,$self->nrow);
  } else {
    @row_numbers = $self->_make_index_positive($rows,$self->nrow);
  }

  my @removed_rows;
  my $p = $self->ncol()-1;
  foreach my $i (@row_numbers) {
    push(@removed_rows,splice(@{$self->{rows}},$i,1));
  }

  if (ref $rows) {
    return \@removed_rows;
  } else {
    return $removed_rows[0];
  }
}

sub copy {
  ### do not use rbind to implement copy()
  my $self = shift;
  my @rows;
  push(@rows,\@$_) foreach @{$self->{rows}};

  return Arrow::DataFrame->new(rows=>\@rows,names=>[@{$self->names}]);
}


sub cbind {
  my $self = shift;
  my $n = $self->nrow; # number of the rows of $self
  my @new_names = @{$self->names};
  my $new_df = Arrow::DataFrame->new(names=>\@new_names);

  if (ref $_[0] eq 'Arrow::DataFrame') {
    my $additional_df = shift;
    my $m = $additional_df->nrow;
    croak "The numbers of rows do not agree. ($n != $m) for cbind()" if $n != $m;

    $new_df->add_name($_) foreach @{$additional_df->names};
    foreach my $i (0..($n-1)) {
      $new_df = $new_df->rbind([@{$self->rows($i)},@{$additional_df->rows($i)}]);
    }
  } else {
    my $col_name = shift;
    my $arrayref = shift;
    my $m = @$arrayref;
    croak "The numbers of rows do not agree. ($n != $m)" if $n != $m;

    $new_df->add_name($col_name);
    foreach my $i (0..($n-1)) {
      $new_df = $new_df->rbind([@{$self->rows($i)},$arrayref->[$i]]);
    }
  }

  if (scalar(@_)>0) { ### there are still data to be added.
    $new_df->cbind(@_);
  } else {
    return $new_df;
  }
}


sub rbind {
  my $self = shift;
  my $additional_data = shift;
  my $new_df = $self->copy;

  if (ref $additional_data eq 'Arrow::DataFrame') {
    ### A data frame as an additional data is given.
    ### Converting each row to a hash, then add it to the data frame.
    my $m = $additional_data->nrow()-1;
    my @names = @{$additional_data->names};

    for my $i (0..$m) {
      my $row_hash = $additional_data->rows_hash($i);
      $new_df = $new_df->rbind($row_hash);
    }

  } elsif (ref $additional_data eq "HASH") {
    ### a hashref is given.
    my @row = map { $additional_data->{$_} } @{$self->names};
    push(@{$new_df->{rows}},\@row);

  } else {
    ### an arrayref is given.
    my @array = @$additional_data;
    my $p = $new_df->ncol;
    croak "The given array (@array) is not suitable for rbind()" if @array != $p;

    ### convert "" to undef
    @array = map { if (defined($_) && $_ eq "") { undef } else { $_ }} @array;
    push(@{$new_df->{rows}},\@array);
  }

  if (scalar(@_)>0) {
    $new_df = $new_df->rbind(@_);
  } else {
    return $new_df;
  }
}


sub merge { ########################################################### merge
  return $_[0];
}


sub rapply {
  my $self = shift;
  my $function = shift; # reference to a subroutine
  my $rows = shift; # a single integer or an arrayreference
  my $range = $self->_give_specified_range($rows,$self->nrow()-1);

  my @results;
  foreach my $i (@$range) {
    my @row = @{$self->rows($i)};
    push(@results, $function->(@row))
  }

  if (!defined($rows) || ref $rows) {
    return \@results;
  } else {
    return $results[0];
  }
}


sub rapply_th {
  my $self = shift;
  my $function = shift; # reference to a subroutine
  my $rows = shift; # a single integer or an arrayreference

  my $names = $self->names;
  my $range = $self->_give_specified_range($rows,$self->nrow()-1);

  my @results;
  foreach my $i (@$range) {
    my @row = @{$self->rows($i)};

    my %row_hash; # ( col0 => val0, col1 => val1, ... )
    foreach my $j (0..$#$names) {
      $row_hash{$names->[$j]} = $row[$j]
    }
    push(@results, $function->(\%row_hash));
  }

  if (!defined($rows) || ref $rows) {
    return \@results;
  } else {
    return $results[0];
  }
}


sub capply {
  my $self = shift;
  my $function = shift;
  my $columns = shift;

  my $cols = $self->names_to_cols($columns);
  my $range = $self->_give_specified_range($cols,$self->ncol()-1);

  my @results;
  # to save memory, do not create all column vectors at the same time.
  foreach my $j (@$range) {
    my $col = $self->cols($j);
    push(@results,$function->(@$col))
  }

  if (!defined($columns) || ref $columns) {
    return \@results;
  } else {
    return $results[0];
  }
}

##################################################### Dealing with missing data
sub complete_cases {
  ### returns a boolean arrayref
  my $self = shift;

}


########################## Converting a wide table to a long one and vice versa
sub melt {
  # $self
  # id | var0 | var1 | var2 | var3 |
  # $self->melt(id=>'id'); # vars => ['var0',...,'var3']

  my $self = shift;
  my %pref = @_;

  croak "" if not defined $pref{id};
  my $id_col_num = $self->names_to_cols($pref{id});   # col number of ID (0)
  my $id_col_name = $self->cols_to_names($id_col_num);# col name of ID (id)

  my $var_index; # col numbers of variables [1,2,3,4]
  if (defined $pref{measure_vars}) {
    $var_index = $self->names_to_cols($pref{measure_vars});
  } else {
    my @names = @{$self->names};
    $var_index = [grep { $names[$_] ne $id_col_name } 0..$#names];
  }

  my $vars = $self->cols_to_names($var_index); ## names of variables 

  my $long = Arrow::DataFrame->new(names=>[$id_col_name,'variable','value']);

  foreach my $i (0..($self->nrow()-1)) {
    my $row = $self->rows_hash($i);
    my $id = $row->{$id_col_name};
    foreach my $var (@$vars) {
      my $val = $row->{$var};
      $long = $long->rbind([$id,$var,$val]) if defined $val;
    }
  }

  return $long;
}


sub dcast {
  my $self = shift;
  my %pref = @_;

  my $id_num = $self->names_to_cols($pref{index});    # column number of IDs
  my $col_num = $self->names_to_cols($pref{columns}); # column number of vars
  my $val_num = $self->names_to_cols($pref{values});  # column number of vals

  my @indexes = uniq @{$self->cols($pref{index})}; # array of IDs
  my @names = uniq @{$self->cols($pref{columns})}; # array of vars
  @names = sort @names; # for compatibility with R
  my @columns = @names; # array of vars (except 'index')
  @names = ($self->cols_to_names($id_num),@names);

  my $wide = Arrow::DataFrame->new(names=>\@names);

  foreach my $index (@indexes) {
    my $subdf = $self->filter(sub { $_[$id_num] eq $index });

    my %row_hash; ##variable => value
    $subdf->rapply(sub { $row_hash{$_[$col_num]} = $_[$val_num] });

    my @row = map {defined $row_hash{$_} ? $row_hash{$_} : $pref{fill}} @columns;
    @row = ($index,@row);
    $wide = $wide->rbind(\@row);
  }

  return $wide;
}


############################## methods corresponding to functions of dplyr in R
sub filter {
  my $self = shift;
  my $function = shift;
  my $n = $self->nrow()-1;
  my $subdf = Arrow::DataFrame->new(names=>\@{$self->names});

  foreach my $i (0..$n) {
    my $row = $self->rows($i);
    if ($function->(@$row)) {
      $subdf = $subdf->rbind($row);
    }
  }
  return $subdf;
}


sub filter_th {
  my $self = shift;
  my $function = shift;
  my $subdf = Arrow::DataFrame->new(names=>\@{$self->names});

  my $n = $self->nrow()-1;
  foreach my $i (0..$n) {
    my $row_hash = $self->rows_hash($i);
    $subdf = $subdf->rbind($self->rows($i)) if $function->($row_hash);
  }
  return $subdf;
}


sub slice {
  my $self = shift;
  my $rows;

  if (ref $_[0] eq 'ARRAY') {
    $rows = $self->rows($_[0]);
  } else {
    $rows = $self->rows(\@_);   
  }

  return Arrow::DataFrame->new(rows=>$rows,names=>\@{$self->names});
}


sub arrange {
  my $self = shift;
  my $n = $self->nrow;

  my @comparison_setting;
  foreach my $item (@_) {
    if (ref $item eq 'ARRAY') {
      my $col_name = shift @$item;
      my %pref = @$item;
      $pref{col} = $self->names_to_cols($col_name);
      $pref{desc} ||= 0;
      $pref{cmp} ||= 0;
      push(@comparison_setting,\%pref);
    } else {
      my $col = $self->names_to_cols($item);
      push(@comparison_setting,{ col=>$col, desc=>0, cmp=>0 });
    }
  }

  my $compare = sub {
    my $cmp = 0;
    foreach my $item (@comparison_setting) {
      my $col = $self->cols($item->{col});
      my $tmp;
	
      if ($item->{cmp} == 1) {
	$tmp = $col->[$a] cmp $col->[$b];
      } elsif (ref $item->{cmp} eq 'CODE') {
	$tmp = $item->{cmp}->($col->[$a],$col->[$b]);
      } else {
	$tmp = $col->[$a] <=> $col->[$b];
      }

      $tmp = -$tmp if $item->{desc};
      $cmp = $cmp || $tmp;
    }
    return $cmp;
  };
  
  my @sorted = sort $compare 0..($n-1);
  return Arrow::DataFrame->new(rows=>$self->rows(\@sorted),names=>\@{$self->names});
}


sub select {
  my $self = shift;
  my @selected;

  while (@_>0) {
    my $item = shift;
    my @given;
    my $remove = "";
    if ($item =~ /^-(.*)$/) {
      $item = $1;
      $remove = "-";
    }
    if ($item =~ /^(.*):(.*)$/) {
      my $from_to = $self->names_to_cols([$1,$2]);
      @given = $from_to->[0]..($from_to->[1]);
    } else {
      $given[0] = $self->names_to_cols($item);
    }
    croak "The string '$remove$item' is invalid for select()." if @given == 0;

    if ($remove eq '-') {
      @selected = 0..($self->ncol()-1) if @selected == 0;

      my $is_given = sub { my $x = shift; grep { $x == $_ } @given };
      @selected = grep { not $is_given->($_) } @selected;

    } else {
      # duplicate is 'not' allowed
      foreach my $x (@given) {
	push(@selected,$x) unless grep { $_ == $x } @selected;
      }
    }
  }

  croak "No columns are (finally) selected." if @selected == 0;
  #@selected = sort { $a <=> $b } @selected;

  my @names = @{$self->names}[@selected];
  my $n = $self->nrow;
  my $new_df = Arrow::DataFrame->new(names=>\@names);

  foreach my $i (0..($n-1)) {
    my @row = @{$self->rows($i)};
    @row = @row[@selected];
    $new_df = $new_df->rbind(\@row);
  }
  return $new_df;
}


sub rename {
  my $self = shift;
  my %new_to_old = @_;
  my @names = @{$self->names};
  my $p = $#names;

  for my $j (0..$p) {
    my @match = grep { $names[$j] eq $new_to_old{$_} } keys %new_to_old;
    $names[$j] = $match[0] if defined $match[0];
  }

  my $new_df = $self->copy();
  $new_df->names(\@names);
  return $new_df;
}


sub distinct {
  my $self = shift;
  my @cols = @{$self->names_to_cols(\@_)};
  my $names = $self->cols_to_names(\@cols);
  my $n = $self->nrow;
  my $p = @cols; ## not $self->ncol;

  my @distinct;
  foreach my $i (0..($n-1)) {
    my @row = @{$self->rows($i)};
    @row = @row[@cols];

    my $duplicate = 0;
    foreach my $registered (@distinct) {
      my @match = grep { $registered->[$_] eq $row[$_]  } 0..($p-1);
      if (@match == $p) {
	$duplicate = 1;
	last;
      }
    }
    push(@distinct,\@row) if not $duplicate;
  }
  return Arrow::DataFrame->new(rows=>\@distinct,names=>$names);
}


sub mutate {
  my $self = shift;
  my @names = @{$self->names};
  my $n = $self->nrow;
  
  my @procedure;
  while (@_) {
    my $col_name = shift;
    if ($col_name =~ /^-?\d+$/) {
      croak "'$col_name' is invalid for a column name." 
    }
    my %proc = (
      name => $col_name,
      num  => $self->names_to_cols($col_name),
      func => shift,
    );
    push(@names,$col_name) if not defined $proc{num};
    push(@procedure,\%proc);
    ### if $col_name is not a column name of $self, then 
    ### names_to_cols($col_name) is undef.
    ### i.e. $col_name is a new column name.
  }

  my @rows;
  foreach my $i (0..($n-1)) {
    my @row = @{$self->rows($i)};

    foreach my $proc (@procedure) {
      my $val;
      if (ref $proc->{func} eq 'CODE') {
	$val = $proc->{func}->(@row);
      } else {
	$val = $proc->{func}
      }
      if (defined $proc->{num}) {
	$row[$proc->{num}] = $val;
      } else {
	push(@row,$val);
      }
    }
    push(@rows,\@row);
  }

  return Arrow::DataFrame->new(rows=>\@rows,names=>\@names);
}

sub mutate_th {
  my $self = shift;
  my @names = @{$self->names};
  my $n = $self->nrow;
  
  my @procedure;
  while (@_) {
    my $col_name = shift;
    if ($col_name =~ /^-?\d+$/) {
      croak "'$col_name' is invalid for a column name." 
    }
    my %proc = (
      name => $col_name,
      num  => $self->names_to_cols($col_name),
      func => shift,
    );
    push(@names,$col_name) if not defined $proc{num};
    push(@procedure,\%proc);
    ### if $col_name is not a column name of $self, then 
    ### names_to_cols($col_name) is undef.
    ### i.e. $col_name is a new column name.
  }

  my @rows;
  my $new_df = Arrow::DataFrame->new(names=>\@names);
  foreach my $i (0..($n-1)) {
    my %row_hash = %{$self->rows_hash($i)};

    foreach my $proc (@procedure) {
      if (ref $proc->{func} eq 'CODE') {
	$row_hash{$proc->{name}} = $proc->{func}->(\%row_hash);
      } else {
	$row_hash{$proc->{name}} = $proc->{func}
      }
    }

    $new_df = $new_df->rbind(\%row_hash);
  }

  return $new_df;
}


sub summarise {
  my $self = shift;
  my @names;
  my @row;

  while (@_) {
    my $item = shift;
    push(@names,$item->[0]);
    push(@row,$self->capply($item->[1],$item->[2]));
  }
  return Arrow::DataFrame->new(rows=>[\@row],names=>\@names);
}

sub summarize {
  my $self = shift;
  return $self->summarise(@_);
}


sub group_by {
  my $self = shift;
  my @levels = map { /^\d+$/ ? $self->cols_to_names->($_) : $_ } @_;

  my %level_values;
  $level_values{$_} = [uniq @{$self->cols($_)}] foreach (@levels);

  my @groups; 
  foreach my $level (@levels) { 
    if (@groups==0) {
      @groups = map { [$_] } @{$level_values{$level}};
    } else {
      my @tuples;
      foreach my $tuple (@groups) {
	push(@tuples,[@$tuple,$_]) foreach (@{$level_values{$level}});
      }
      @groups = @tuples;
    }
  }

  # if v5.10 or later is available, we can use smart match.
  my $value_check = sub {
    my $group = shift;
    my $row_hash = shift;
    my @vals = map { $row_hash->{$_} } @levels; # values at level columns

    my $agree = 1;
    foreach my $j (0..$#vals) {
      if ($group->[$j] ne $vals[$j]) {
	$agree = 0;
	last;
      }
    }
    return $agree;
  };

  my @data;
  my @minuslevel = map { "-$_" } @levels;
  foreach my $group (@groups) {
    my $subset = $self->filter_th(sub {$value_check->($group,$_[0])});
    push(@data,[@$group,$subset->select(@minuslevel)]) if $subset->nrow != 0;
  }

  return Arrow::GroupBy->new(levels=>\@levels,data=>\@data);
}

########################################################################### misc

### - `$df->rearrange({rows=>[9,0,3],columns=>[1,0,2]})`
### sth like df[c(9,0,3),c(1,0,2)] in R
### $df->rearrange($df->order())
### This does not change $df itself. 
sub rearrange {
  my $self = shift;
  my $pref = shift;

  my $n = $self->nrow()-1;
  my $p = $self->ncol()-1;
  $pref->{rows} ||= [0..$n];
  $pref->{cols} ||= [0..$p];

  my @names = @{$self->names()};
  @names = @names[@{$pref->{cols}}];

  my $df = DataFrame->new(names=>\@names);
  foreach my $i (@{$pref->{rows}}) {
    my @row = @{$self->rows($i)};
    my @row_selected = @row[@{$pref->{cols}}];
    $df->rbind(\@row_selected);
  }
  return $df;
}

### as_matrix([2,3],[0,4,5]) returns the following 2x3 matrix
### x20 x24 x25
### x30 x34 x35
### empty arrayref [] corresponds all rows/columns.
### Remark: undef will be automatically converted into 0.
sub as_matrix {
  my $self = shift;
  my $rows = shift;
  my $cols = shift;

  if (!$rows || !@$rows) {
    my $n = $self->nrow();
    $n--;
    $rows = [0..$n];
  }
  if (!$cols || !@$cols) {
    my $p = $self->ncol();
    $p--;
    $cols = [0..$p];
  }

  my @M;
  foreach my $i (@$rows) {
    my @row = @{$self->rows($i)}[@$cols];     
    @row = map { $_ || 0 } @row; # undef -> 0
    push(@M,\@row);
  }

  return Math::MatrixReal->new_from_rows(\@M);
}


### ---------------------------------------------------- 
### - `$df->near_zero_var()`
###   nearZeroVar() of caret in R.
###   - freq_cut : the cutoff for the ratio (freq_ratio) of 
###     the most common value to the second most common value
###     (if the second one is 0, then the first one is the ratio)
###   - uniqueCut : the cutoff for the percentage (percent_unique) of
###     distinct values out of the number of total samples

sub near_zero_var {
  my $self = shift;
  my $pref = shift;
  $pref->{freq_cut} ||= 95/5;
  $pref->{unique_cut} ||= 10;
  $pref->{save_metrics} ||= 0; # not implimented yet

  my $p = $self->ncol()-1;
  my $n = $self->nrow();

  my @freq_ratio;
  my @percent_unique;
  my @zero_var;
  my @nzv;

  foreach my $j (0..$p) {
    my @col = grep { defined $_ } @{$self->columns($j)};
    my @ucol = uniq @col;
    $percent_unique[$j] = 100 * @ucol / $n;

    if (@col <= 1) {
      ### only undef (0) or the same value (1)
      $zero_var[$j] = 1;
      $freq_ratio[$j] = 0;
    } else {
      $zero_var[$j] = 0;

      my %counter = ();
      foreach my $val (@col) {
	### count the appearing values
	$counter{"$val"}++;
      }
      ### array of counts in descending order
      my @values = sort { $b<=>$a } map { $counter{$_} } @ucol;
      $freq_ratio[$j] = $values[0]/$values[1];
    }

    if ($freq_ratio[$j] > $pref->{freq_cut} && $percent_unique[$j] <= $pref->{unique_cut} or $zero_var[$j]) {
      $nzv[$j] = 1;
    } else {
      $nzv[$j] = 0;
    }
  }

  if ($pref->{save_metrix}) {
    my $hashref = {
      column         => $self->names,
      freq_ratio     => \@freq_ratio,
      percent_unique => \@percent_unique,
      zero_var       => \@zero_var,
      nzv            => \@nzv,
    };
    return DataFrame->hash_to_dataframe($hashref,[qw(column freq_ratio percent_unique zero_var nzv)]);
  } else {
    return [grep { $nzv[$_] } 0..$p];
  }
}




### -------------------------------------------------------- "private" methods
### The following methods should not be used outside 

### `_make_index_positive(@arr,$n)`;
### make given indexes of an "original" array positive 
### and sort them in decsending order
sub _make_index_positive {
  my $self = shift;
  my $n = splice(@_,-1,1); # number of the "original" array
  my @arr = @_; # subarray which should be sorted

  # make it positive: e.g. -1 -> k-1 
  @arr = map { if ($_<0){$_+$n} else {$_} } @arr;
  # remove stange assignments 
  @arr = grep{ 0 <= $_ && $_ < $n} @arr;
  # remove duplicated numbers 
  my %tmp = map { $_=>1 } @arr;
  # sort descending order
  @arr = sort { $b <=> $a } keys %tmp;

  return @arr;
}

### - `_give_specified_range($specified,$max)`
### $specified == 4     -> [$specified]  (exception)
### $specified == [1]   -> $specified    
### $specified == [1,2] -> $specified
### $specified == undef -> [0..$max]
### This is an exception of the output rule
sub _give_specified_range {
  my $self = shift;
  my $specified = shift;
  my $max = shift;

  if (defined $specified) {
    if (ref $specified) {
      return $specified;
    } else {
      return [$specified]
    }
  } else {
    return [0..$max];
  }
}


=pod

=head1 NAME

Arrow::DataFrame - something like a data.frame in R.

=head1 SUMMARY

This Perl module provides a degenerated variant of a data frame in R.
The names of methods follow mostly ones in R.
Only very basic functions are currently implemented. 

=head1 SYNOPSIS

   use Arrow::DataFrame;

   ### read a CSV file 
   my $df = Arrow::DataFrame->read_csv($csv_file);

   ### show first 10 rows of the data frame.
   $df->head(10);

=head1 DESCRIPTION

=head2 DataFrame

A data frame consists of a (n by p) matrix and names of columns.

   my $df = Arrow::DataFrame->new(
     names => [qw(col0 col1 col2 col3)],
     rows  => [[0,1,2,3],[4,5,6,7]],
   );

The above code generate the following data frame.

       | col0   | col1   | col2   | col3  # names of columns 
   000 | 0      | 1      | 2      | 3     # 
   001 | 4      | 5      | 6      | 7     # 

In the above code `names` is the arrayref to names of columns and
`rows` is a arrayref to arrayrefs to the matrix. 

=head2 Rules for a data frame

Here we describe basic rules for a data frame. Some are general but others
are only for this module.

1. Each row corresponds an observation (or a sample).

2. Each column corresponds a 'variable'. Each column consists of values of the variable for observations. For example a column which is probably named 'price' consists of the price of each observation.

One of the conclusion of this rule is: each column should consist of elements of the same type. For example the first column consists only of integers and the second column consists only of strings.

3. The indexes of columns and rows B<start from 0> unlike R.

4. The names of columns must be distinct from each other. Namely the following data frame is B<NOT allowed>. 

   my $df = Arrow::DataFrame->new(
     names => [qw(xa xb xa)],     # 'xa' is duplicated
     rows  => [[1,2,3],[4,5,6]],
   );

This causes a problem when we use a column name. 

5. A name of a column must not consist only of numbers. 

   my $df = Arrow::DataFrame->new(
     names => [qw(xa xb 2015)],   # '2015' is not allowed
     rows  => [[1,2,3],[4,5,6]],
   );

6. Each element of an array in "rows" is assumed to be a numerical/string literal. (Most of methods work "properly", even though an element in a row is a (blessed) reference. But, for example, this causes a problem when saving a data frame as a CSV file.)

=head2 Rules for methods

1. When specifying more than 1 rows or columns, we use an arrayref. 

   my @cols = qw(col0 col2);
   $df->method(\@cols); # ok
   $df->method(@cols);  # no

  But the functions corresponding to the dplyr do not have to follow the above rules.

2. When specifying more than 1 columns, we may use of names (e.g. col0, col2) or indexes (e.g. 0, 2). But it is not allowed to use a name and an index at the same time.

   $df->method(['col0','col2']); # ok 
   $df->method([0,2]);           # also ok
   $df->method(['col0',2]);      # no

3. When specifying only one row or column, we can use a scalar or an arrayref to one scalar.

   $df->method('col1');   # ok 
   $df->method(['col1']); # ok
   $df->method(1);        # ok
   $df->method([1]);      # ok

4. The type of the output follows one of the input

   $df->method('2');   # something2  (This might be an arrayref.)
   $df->method([2]);   # [something2]
   $df->method([2,3]); # [something2,something3]

5. A reference to a subroutine in an argument of a method takes B<an array> rather than an arrayref. In the code

   $df->method(\&func);

func() must take an array. (Its return value depends on the method.)

6. Some methods taking a function reference has a variant: method_th(). Here "th" stands for "through hash". For this method the refered function takes B<only a hashref>. The following is a typical code

   $df->method_th(sub{ $_[0]{'col2'} == 2015 });

or 

   $df->method_th(sub{ my $row = shift; $row{'col2'} == 2015 });

7. A return value of a method should always be a scalar.



=head1 METHOD

=head3 Notation 

We use the following variables for methods.

=over 1

=item $cols  : a scalar or arrayref for column names or indicies

=item $rows  : a scalar or arrayref for row indicies

=item $names : a scalar or arrayref for names of columns

=item $pref  : a hashrerf for preference

=item \&func : a reference to a subroutine

=item $dbh   : a database handle object (a DBI object)

=item $sql   : an SQL command (with a place holder)

=back

The following variables are used for implementation.

=over 1

=item $n : the number of rows (observations, samples).
      Thus the range of index of rows is from 0 to $n-1.

=item $p : the number of columns (predictors, features, covariables)
      Thus the range of index of columns is from 0 to $p-1.
      It is usual that a target variable, which is namely not a predictor, is one
      of the columns of a data frame (in a context of statistical learning). But
      the pair ($m,$n) is pretty confusing. So we use ($n,$p) instead.

=item $i : an index of a row

=item $j : an index of a column

=back

=head2 Constructors

=head3 new 

   $df = Arrow::DataFrame->new(names=>[col0,...], rows=>[[x00,...,x0p],...]);

If "names" is not given and "rows" is given, then [X1,...,Xp] is assigned to it.
This constructor does not check the length of each row.

=head3 hash_to_dataframe

   $df = Arrow::DataFrame->hash_to_dataframe($hashref,$names);

This converts a hash to an instance of DataFrame. The values of the referenced hash must be an arrayref:

   $hashref = { col0 => [x10,...,xn0], col1 => [x01,...,xn2], ... }

The number of rows will be equal to the length of the 1st column. B<Every element
of @$names should be a key of %$hashref> and $names determins the order of the 
columns. If $names is not given, then the (sorted) hash keys will be used.

Here is an example.

   my $dg = Arrow::DataFrame->hash_to_dataframe(
     { x0 => [10,11], x1 => ['a','b'] },
     ['x0','x1']
   );

The above code produces the following data frame.

   #     | x0     | x1    
   # 000 | 10     | a     
   # 001 | 11     | b   

=head3 read_csv

   my $df = Arrow::DataFrame->read_csv($file,$pref);

This reads a CSV file and creates a data frame. $pref is used as a preference for Text::CSV B<except 'header' key>. 

   my $df = Arrow::DataFrame->read_csv($file,{ header=>1 }); # default

This uses the first line of the CSV file as names of columns (default). If the first line is not for the names, 'header' should be '0' and "X1", "X2", ... are used as names of columns.

=head3 read_sql

  my $df = Arrow::DataFrame->read_sql($dbh,$sql,$var);

This reads a SQL database and creates a data frame. Note that B<all described rows are fetsched>. $var is an arrayref for the place holder. 

  my $sql = "select * from Table where price > ?";
  my $df = Arrow::DataFrame->read_sql($dbh,$sql,[2000]);

=head3 matrix_to_dataframe

  my $df = Arrow::DataFrame->matrix_to_dataframe($matrix,$names);

This converts an object of Math::MatrixReal to a data frame. If the number of columns does not agree with the number of given names, "X1", "X2", ... are used for the name of columns.

In near future PDL is used instead of Math::MatrixReal.

=head2 Getter and Setter

=head3 names and set_names

An arrayref to the names of columns. 

   $df->names;            # gives the arrayref to names of columns
   $df->names($arrayref); # assigns $arrayref to the new names of column.

If you want to a new name to an n-th column, then we use `set_name` instead.

   $df->set_name("id",0); # assigns "id" to the name of the first column.

   #     | id     | col1   | col2   | col3  
   # 000 | 0      | 1      | 2      | 3     
   # 001 | 4      | 5      | 6      | 7  

The retured value of `set_name()` is the arrayref to the names of columns.

=head3 separator

The separetor is a letter which is used when showing the data frame. The default value is a vertical bar:

   #     | col0   | col1   | col2   | col3  
   # 000 | 0      | 1      | 2      | 3     
   # 001 | 4      | 5      | 6      | 7    

   $df->separator;      # gives the current separator.
   $df->separator(" "); # sets the separator to one space. 

=head3 width

The width of each column to show is fixed. The default value is "6". 

   $df->width;       # gives the current width.
   $df->width("10"); # sets the width to 10.

After the second line, a data frame is showed as follows.

   #     | col0       | col1       | col2       | col3      
   # 000 | 0          | 1          | 2          | 3         
   # 001 | 4          | 5          | 6          | 7    

=head2 Write a data frame 

=head3 write_csv

This method produces a CSV file of the data frame (in UTF-8 encoding).

   $df->write_csv("file_name.csv");

=head2 Read a data frame

=head3 nrow and ncol

   $df->nrow; # give the number of rows in the data frame
   $df->ncol; # give the number of columns in the data frame

=head3 show

This method shows rows of the data frames. We do not need `print` function.

   $df->show; # shows the whole data frame.
   $df->show(from=>1,to=>9,width=>4,separator=>"#");

The second line shows the data frame from the 2nd row to the 10th row, assigning temporally the width and separator character.

The `head` option is also available:

   $df->show(head=>4);  # shows the first 4 rows.
   $df->show(head=>-3); # shows the last 3 rows.

Note that `head` has priority over `from` and `to` options.

=head3 head and tail

   $df->head(9); ### equivalent to $df->show(head=>9)
   $df->tail(9); ### equivalent to $df->show(head=>-9)

=head3 names_to_cols($cols)

This converts names of columns to the corresponding column numbers.

   $df->names_to_cols("col3");          # gives 3
   $df->names_to_cols(["col0"]);        # gives [0]
   $df->names_to_cols(["col2","col1"]); # gives [2,1]

This method does nothing for an integer (so that other methods still work
even though $cols contains a column name and a column number). If there is
no corresponding column number, then returns undef. This method can be used 
with `defined()` to check whether a sting appears in $df->{names} or not.

=head3 cols_to_names($cols)

This converts column numbers to the corresponding column names.

   $df->cols_to_names([3,1]);              # gives ['col3','col1']
   $df->cols_to_names([0..$df->ncol()-1]); # equiv to $df->names;

If we give a string which does not belong to names, then it produces an error.

=head3 rows($rows)

This gives the specified rows ($rows).

   $df->rows(3);     # gives arrayref to the element of 3rd row
   $df->rows([3]);   # gives [$df->rows(3)]
   $df->rows([1,2]); # gives [$df->rows(1) ,$df->rows(2)]

The return value is an arrayref, not a data frame. Note that the returned values are copy of the original.

=head3 rows_hash($rows)

This converts the specified rows into hashrefs. 

   $df->rows_hash(0);

gives the hashref `{'col0'=>0, 'col1'=>1, 'col2'=>2, 'col3'=>3}`.

   $df->rows_hash([3,4]); # gives [$df->rows_hash(3),$df->rows_hash(4)]

=head3 cols($col)

This gives the arrayref to the specified columns.

   $df->cols(2);     # gives the arrayref to the elements in the 3rd column.
   $df->cols([2]);   # [$df->cols(2)];
   $df->cols([1,2]); # [$df->cols(1),$df->cols(2)];

=head3 at

This method picks the value of the given coordinate. For example

   $df->at([1,2]);

gives the value in the second row and the third column.

=head2 Tweaking a data frame

=head3 add_name

This method is only for writing a method for this module. If "col9" does not 
appear in $df->{names}, then 

   $df->add_name("col9");

just adds "col9" at the end of $df->{names} and its column number is returned. 
If "col9" appears in $df->{names}, then "undef" is returned. This method accepts
only one string.

=head3 del_cols($cols)

This method removes the specified columns from the data frame and returns 
the B<removed columns>, not the data frame without the specified columns. 
Namely 

   $df->remove_cols("col2")

returns the same value as `$df->cols("col2")`.

This method accepts a negative integer to specify a column. For example 
-1 means the last column.

=head3 del_rows($rows)

This method removes the specified rows from the data frame and returns 
the removed rows.

=head3 copy

This method gives a copy of a data frame.

   my $dg = $df->copy;

=head3 cbind

This method gives a B<new data frame> to which a given column is added.

   $df = $df->cbind(col_name=>$arrayref);

This line adds a new column "col_name" to $df so that $df->cols("col_name")
is exactly $arrayref. If the length of @$arrayref and the number of rows of
$df do not agree, then the method will fail.

This method also accepts a data frame

   $df->cbind($dg);

The numbers of rows are also checked.

=head3 rbind

This method gives a new data frame which we obtain by adding given data to the data frame. The easiest example is 

   $df = $df->rbind([x1,...,xp]);

Then the last row of `$df` becomes `[x1,...,xp]`.

This method accepts a data frame and a hash as well.

   $df->rbind($dg);
   $df->rbind({col1=>x1,col2=>x2,...});

If a column name of `$df` can not be found in the column names of `$dg` (or keys
of the hash), then undef is used for the corresponding values.

=head3 merge

Not yet implemented.

=head3 rapply(\&func) and rapply_th(\&func)

This is a degenerate version of `apply(df,1,func)` in R: The function `func()` takes each array (not an arrayref) and must give a scalar
value. For example 

   $df->rapply(sub{ $_[0] + $_[1] });

gives an array consisting of the sum of the first and second elements of rows.

When we want to use the column names, we use `rapply_th()`. 

   $df->rapply_th(sub{ $_[0]{col0} + $_[0]{col1} })

Note that the `$_[0]` is the reference to the hash of a row.

=head3 capply(\&func,$cols)

This is a similar function to `apply(df,2,func)` in R: This method apply the function reference to spcified columns. The function reference takes an array of a column and must give a scalar value.

   $df->capply(sub{ my $s; $s += $_ foreach @_; $s; }, "col0");

gives the sum of all elements of the column "col0". If we omit $cols, then \&func is applied to all columns.

Note that the order of `\&func` and `$cols`.

=head2 Dealing with missing data

=head3 complete_cases

Not yet.

=head2 Converting a wide table to a long one and vice versa

=head3 melt

This method converts a wide table to a long one.

   $df->melt(id=>'col0',measure_vars=>$cols);

The column names of the melted data frame would be ['col0','variable','value'].

=head3 dcast

This method converts a long table to a wide one. 

   $df->dcast(index=>'col0',columns=>'col1',values=>'col2',fill=>'0');

(We follow the usage of pandas' pivot()-method.) If the 'fill' option is given, then the empty cells are filled by the specified value.

=head2 methods corresponding to functions of dplyr in R

The methods which are explained in this section B<create a new data frame>. 

=head3 filter(\&func) and filter_th(\&func)

This method creates a new data subframe consisting of rows on which \&func gives true. The return value of \&func is used as a boolean value. For example

   $df->filter(sub{ $_[1] == 17 });

gives the data subframe consisting of the rows whose second element is equal to 17 and 

   $df->filter(sub{ $_[0]{'name'} =~ /honda/ });

gives the data subframe consisging of the rows whose values of "name" contain "honda".

=head3 slice

This creates the data subframe consisting of the specified rows. For example

   $df->slice(0..9);

gives the data subframe consisting of the first 10 rows of $df. This method also accepts an arrayref.

=head3 arrange

This methods arranges the rows. An easiest usage is 

   $df->arrange('year','month','day');

This gives a new data frame which is sorted by columns 'year', 'month' and 'day'. The default comparison is "<=>", i.e.

=over 1

=item The rows are sorted in the ascending order.

=item The elemens of the specified column are assumed to be numeric.

=back

To sort the rows in descending order, put `["col1",desc=>1]` instead of "col1". To use "cmp" for comparison, put `["col1",cmp=> ...]` intead. The value of cmp accepts also a function refernce. For example, 

   $df->arrange(["name",desc=>1,cmp=>sub {$_[0] cmp $_[1]}]);

is equivalent to 

   $df->arrange(["name",desc=>1,cmp=>1]);

=head3 select

This restrict columns to specified ones. For example

   $df->select('col1','col3');

consists of two columns 'col1' and 'col3' of the original data frame $df. We may use ':' to specify continued columns.

   $df->select('col1:col3');

would be equivalent to 

   $df->select('col1','col2','col3');

We may use the column numbers, but it must be a string literal if you use `:` with it.

   $df->select('1:3'); # `$df->select(1:3)` does not work.

Note that the above line is equivalent to 

   $df->select(1,2,3); # or $df->select(1..3) for short.

Namely the fourth column `$df->columns(3)` is contained in `$df->select('1:3')`.

If the string literal starts with `-`, then the specified column is removed. For example

   $df->select('col0:col2','-col1');

is equivalent to `$df->select('col0','col2')`. If the first argument starts with '-', for example

   $df->select('-0'); ### do not forget single quotations.

then the result is the original data frame without the first column.

=head3 rename(new_name0=>old_name0,new_name1=>old_name1,...)

This change the column names. Note that the new names are the keys of the argument of the method. If a specified old name does not belong to the column names, then it is ignored.

Use `set_name()` method, when modifying column names to remove invalid column names. The `rename()` method assumes that there is no invalid column name.


=head3 distinct 

This method is quite different from distinct of dplyr, but similar to DISTINCT of SQL. For example

   $df->distinct("col0","col2");

consists of two columns and its rows are distinct values of the two columns. The above line is equivalent to `distinct(select(df,col0,col2))` in R.

=head3 mutate() and mutate_th()

This method applying a function to each rows and adds the returned value as a new column or replaces the old value with the returned value. An easiest usage is 

   $df->mutate('hoge', \&func);

If 'hoge' is not a column name of $df, then this is equivalent to 

   $df->cbind('hoge'=>$df->rapply(\&func));

If 'hoge' is a column name of `$df`, then the values of `$df->cols('hoge')` are replaced with the values of `$df->rapply(\&func)`.

We can add pairs of a (new) column name and a function reference as follows:

   $df->mutate('col5',\&func5,'col6',\&func6,'col7',\&func7,...);

Note that the returned value of `\&func5` is available when we apply `\&func6`. Therefore you should be very careful if you use a negative index in the function reference. For example

   $df->mutate('new_col0', sub { rand(1) }, 'new_col1', sub { $_[-1] });

If 'new_col0' is not a column name of $df, then the column 'new_col1' of the produced data frame is same as the column 'new_col0'.

When we want to use column names for the function references, use `mutate_th()`.

=head3 transmute() and transmute_th()

Not yet implemented. Use mutate() and select().

=head3 summarise() (or summarize())

This method produce a data frame consisting of a single row. In an example

   $df->summarise( ['ave', \&func, 'col2'] );

we apply an aggregate function \&func to the colunm 'col2', then its returned value is the unique value of the column 'ave' of the new data frame. Thus the above line is equivalent to 

   Arrow::DataFrame->new(rows=>[[$df->capply(\&func,'col2')]],names=>['ave']);

We can give several triples:

   $df->summarise( ['ave0', \&func0, 'col0'] , ['ave1', \&func1, 'col1'], ...);

=head2 GroupBy object

Roughly speaking, a (Arrow::)GroupBy object is an object like a data frame such that the last elements of rows are data frame with the same column names. The name of the last column is 'data_frame' and other column is called a 'level'. (This name is of course not general.) This object can be used to group a data frame by the values of specified columns.

Note that B<a GroupBy object is not a DataFrame object>.

=head3 group_by

This method creates a GroupBy object. 

   $grpd = $df->group_by('origin','cylinders');

Then $grpd contains a data frame whose colunm names are 'origin', 'cylinders' and 'data_frame'. The first two columns (called 'levels') consist of distinct pairs of values of the two columns of $df. If the first two elements of a row is 1 and 8, then the third (last) element is a data frame given by 

   $df->filter_th(sub {$_[0]{origin} == 1 && $_[0]{cylinders} == 8});

=head3 level_values

   $grpd->level_values;

This produces the data frame consisting only of levels columns. (The data frame is same as $df->distinct('origin','cylinders').)

=head3 cat

This method is basically the inverse of group_by() (up to orders of columns). 

   $grpd->cat;

This produces a single data frame consisting of grouped data frames (and the level columns). 

=head3 summarise (or summarize)

This method applies an aggregate function to the column of each grouped data frame and show the result as a single data frame.

   $grpd->summarise(['sum_mpg',\&sum,'mpg']);

This creates a data frame whose last column consists of the sums of the columns 'mpg' of all grouped data frame (if &sum is suitably defined). The column name is 'sum_mpg'.

=head3 filter(_th), slice, arrange, select, rename, distinct, mutate(_th)

These methods give a GroupBy object. They apply the method of the same name as ones for DataFrame to each grouped data frame. The group whose data frame has no rows will be removed. For example 

   $grpd->level_values;

contains 9 rows, while

   $grpd->filter_th(sub { $_[0]{name} =~ /honda/ })->level_values;

contains only one row.


=head1 TODO

=over 1

=item More functions! 

=item read_csv should accept a URL and deal with an NA character.

=item The output of del_cols (and del_rows) should be a data frame or a hash

=item read_csv should accept a reference to convert elements of a column.

=item to_json, read_json (so that they are compatible with the functions corresponding to R )

=item Make this independent of Arithmetic.pm.

=item Consider indices of rows.

=item use PDL

=back

=head1 AUTHOR

stdiff <hsakai@stdiff.net>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2015 by stdiff.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

1;
