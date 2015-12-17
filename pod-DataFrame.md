# NAME

Arrow::DataFrame - something like a data.frame in R.

# SUMMARY

This Perl module provides a degenerated variant of a data frame in R.
The names of methods follow mostly ones in R.
Only very basic functions are currently implemented. 

# SYNOPSIS

    use Arrow::DataFrame;

    ### read a CSV file 
    my $df = Arrow::DataFrame->read_csv($csv_file);

    ### show first 10 rows of the data frame.
    $df->head(10);

# DESCRIPTION

## DataFrame

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

## Rules for a data frame

Here we describe basic rules for a data frame. Some are general but others
are only for this module.

1\. Each row corresponds an observation (or a sample).

2\. Each column corresponds a 'variable'. Each column consists of values of the variable for observations. For example a column which is probably named 'price' consists of the price of each observation.

One of the conclusion of this rule is: each column should consist of elements of the same type. For example the first column consists only of integers and the second column consists only of strings.

3\. The indexes of columns and rows **start from 0** unlike R.

4\. The names of columns must be distinct from each other. Namely the following data frame is **NOT allowed**. 

    my $df = Arrow::DataFrame->new(
      names => [qw(xa xb xa)],     # 'xa' is duplicated
      rows  => [[1,2,3],[4,5,6]],
    );

This causes a problem when we use a column name. 

5\. A name of a column must not consist only of numbers. 

    my $df = Arrow::DataFrame->new(
      names => [qw(xa xb 2015)],   # '2015' is not allowed
      rows  => [[1,2,3],[4,5,6]],
    );

6\. Each element of an array in "rows" is assumed to be a numerical/string literal. (Most of methods work "properly", even though an element in a row is a (blessed) reference. But, for example, this causes a problem when saving a data frame as a CSV file.)

## Rules for methods

1\. When specifying more than 1 rows or columns, we use an arrayref.

    my @cols = qw(col0 col2);
    $df->method(\@cols); # ok
    $df->method(@cols);  # no

2\. When specifying more than 1 columns, we may use of names (e.g. col0, col2) or indexes (e.g. 0, 2). But it is not allowed to use a name and an index at the same time.

    $df->method(['col0','col2']); # ok 
    $df->method([0,2]);           # also ok
    $df->method(['col0',2]);      # no

3\. When specifying only one row or column, we can use a scalar or an arrayref to one scalar.

    $df->method('col1');   # ok 
    $df->method(['col1']); # ok
    $df->method(1);        # ok
    $df->method([1]);      # ok

4\. The type of the output follows one of the input

    $df->method('2');   # something2  (This might be an arrayref.)
    $df->method([2]);   # [something2]
    $df->method([2,3]); # [something2,something3]

5\. A reference to a subroutine in an argument of a method takes **an array** rather than an arrayref. In the code

    $df->method(\&func);

func() must take an array. (Its return value depends on the method.)

6\. Some methods taking a function reference has a variant: method_th(). Here "th" stands for "through hash". For this method the refered function takes **only a hashref**. The following is a typical code

    $df->method_th(sub{ $_[0]{'col2'} == 2015 });

or 

    $df->method_th(sub{ my $row = shift; $row{'col2'} == 2015 });

7\. A return value of a method should always be a scalar.

# METHOD

### Notation 

We use the following variables for methods.

- $cols  : a scalar or arrayref for column names or indicies
- $rows  : a scalar or arrayref for row indicies
- $names : a scalar or arrayref for names of columns
- $pref  : a hashrerf for preference
- \&func : a reference to a subroutine
- $dbh   : a database handle object (a DBI object)
- $sql   : an SQL command (with a place holder)

The following variables are used for implementation.

- $n : the number of rows (observations, samples).
      Thus the range of index of rows is from 0 to $n-1.
- $p : the number of columns (predictors, features, covariables)
      Thus the range of index of columns is from 0 to $p-1.
      It is usual that a target variable, which is namely not a predictor, is one
      of the columns of a data frame (in a context of statistical learning). But
      the pair ($m,$n) is pretty confusing. So we use ($n,$p) instead.
- $i : an index of a row
- $j : an index of a column

## Constructors

### new 

    $df = Arrow::DataFrame->new(names=>[col0,...], rows=>[[x00,...,x0p],...]);

If "names" is not given and "rows" is given, then [X1,...,Xp] is assigned to it.
This constructor does not check the length of each row.

### hash_to_dataframe

    $df = Arrow::DataFrame->hash_to_dataframe($hashref,$names);

This converts a hash to an instance of DataFrame. The values of the referenced hash must be an arrayref:

    $hashref = { col0 => [x10,...,xn0], col1 => [x01,...,xn2], ... }

The number of rows will be equal to the length of the 1st column. **Every element
of @$names should be a key of %$hashref** and $names determins the order of the 
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

### read_csv

    my $df = Arrow::DataFrame->read_csv($file,$pref);

This reads a CSV file and creates a data frame. $pref is used as a preference for Text::CSV **except 'header' key**. 

    my $df = Arrow::DataFrame->read_csv($file,{ header=>1 }); # default

This uses the first line of the CSV file as names of columns (default). If the first line is not for the names, 'header' should be '0' and "X1", "X2", ... are used as names of columns.

### read_sql

    my $df = Arrow::DataFrame->read_sql($dbh,$sql,$var);

This reads a SQL database and creates a data frame. Note that **all described rows are fetsched**. $var is an arrayref for the place holder. 

    my $sql = "select * from Table where price > ?";
    my $df = Arrow::DataFrame->read_sql($dbh,$sql,[2000]);

### matrix_to_dataframe

    my $df = Arrow::DataFrame->matrix_to_dataframe($matrix,$names);

This converts an object of Math::MatrixReal to a data frame. If the number of columns does not agree with the number of given names, "X1", "X2", ... are used for the name of columns.

In near future PDL is used instead of Math::MatrixReal.

## Getter and Setter

### names and set_names

An arrayref to the names of columns. 

    $df->names;            # gives the arrayref to names of columns
    $df->names($arrayref); # assigns $arrayref to the new names of column.

If you want to a new name to an n-th column, then we use `set_name` instead.

    $df->set_name("id",0); # assigns "id" to the name of the first column.

    #     | id     | col1   | col2   | col3  
    # 000 | 0      | 1      | 2      | 3     
    # 001 | 4      | 5      | 6      | 7  

The retured value of `set_name()` is the arrayref to the names of columns.

### separator

The separetor is a letter which is used when showing the data frame. The default value is a vertical bar:

    #     | col0   | col1   | col2   | col3  
    # 000 | 0      | 1      | 2      | 3     
    # 001 | 4      | 5      | 6      | 7    

    $df->separator;      # gives the current separator.
    $df->separator(" "); # sets the separator to one space. 

### width

The width of each column to show is fixed. The default value is "6". 

    $df->width;       # gives the current width.
    $df->width("10"); # sets the width to 10.

After the second line, a data frame is showed as follows.

    #     | col0       | col1       | col2       | col3      
    # 000 | 0          | 1          | 2          | 3         
    # 001 | 4          | 5          | 6          | 7    

## Write a data frame 

### write_csv

This method produces a CSV file of the data frame (in UTF-8 encoding).

    $df->write_csv("file_name.csv");

## Read a data frame

### nrow and ncol

    $df->nrow; # give the number of rows in the data frame
    $df->ncol; # give the number of columns in the data frame

### show

This method shows rows of the data frames. We do not need `print` function.

    $df->show; # shows the whole data frame.
    $df->show(from=>1,to=>9,width=>4,separator=>"#");

The second line shows the data frame from the 2nd row to the 10th row, assigning temporally the width and separator character.

The `head` option is also available:

    $df->show(head=>4);  # shows the first 4 rows.
    $df->show(head=>-3); # shows the last 3 rows.

Note that `head` has priority over `from` and `to` options.

### head and tail

    $df->head(9); ### equivalent to $df->show(head=>9)
    $df->tail(9); ### equivalent to $df->show(head=>-9)

### names_to_cols($cols)

This converts names of columns to the corresponding column numbers.

    $df->names_to_cols("col3");          # gives 3
    $df->names_to_cols(["col0"]);        # gives [0]
    $df->names_to_cols(["col2","col1"]); # gives [2,1]

This method does nothing for an integer (so that other methods still work
even though $cols contains a column name and a column number). If there is
no corresponding column number, then returns undef. This method can be used 
with `defined()` to check whether a sting appears in $df->{names} or not.

### cols_to_names($cols)

This converts column numbers to the corresponding column names.

    $df->cols_to_names([3,1]);              # gives ['col3','col1']
    $df->cols_to_names([0..$df->ncol()-1]); # equiv to $df->names;

### rows($rows)

This gives the specified rows ($rows).

    $df->rows(3);     # gives arrayref to the element of 3rd row
    $df->rows([3]);   # gives [$df->rows(3)]
    $df->rows([1,2]); # gives [$df->rows(1) ,$df->rows(2)]

The return value is an arrayref, not a data frame. Note that the returned values are copy of the original.

### rows_hash($rows)

This converts the specified rows into hashrefs. 

    $df->rows_hash(0);

gives the hashref `{'col0'=>0, 'col1'=>1, 'col2'=>2, 'col3'=>3}`.

    $df->rows_hash([3,4]); # gives [$df->rows_hash(3),$df->rows_hash(4)]

### cols($col)

This gives the arrayref to the specified columns.

    $df->cols(2);     # gives the arrayref to the elements in the 3rd column.
    $df->cols([2]);   # [$df->cols(2)];
    $df->cols([1,2]); # [$df->cols(1),$df->cols(2)];

### at

This method picks the value of the given coordinate. For example

    $df->at([1,2]);

gives the value in the second row and the third column.

## Tweaking a data frame

### add_name

This method is only for writing a method for this module. If "col9" does not 
appear in $df->{names}, then 

    $df->add_name("col9");

just adds "col9" at the end of $df->{names} and its column number is returned. 
If "col9" appears in $df->{names}, then "undef" is returned. This method accepts
only one string.

### del_cols($cols)

This method removes the specified columns from the data frame and returns 
the **removed columns**, not the data frame without the specified columns. 
Namely 

    $df->remove_cols("col2")

returns the same value as `$df->cols("col2")`.

This method accepts a negative integer to specify a column. For example 
\-1 means the last column.

### del_rows($rows)

This method removes the specified rows from the data frame and returns 
the removed rows.

### copy

This method gives a copy of a data frame.

    my $dg = $df->copy;

### cbind

This method gives a **new data frame** to which a given column is added.

    $df = $df->cbind(col_name=>$arrayref);

This line adds a new column "col_name" to $df so that $df->cols("col_name")
is exactly $arrayref. If the length of @$arrayref and the number of rows of
$df do not agree, then the method will fail.

This method also accepts a data frame

    $df->cbind($dg);

The numbers of rows are also checked.

### rbind

This method gives a new data frame which we obtain by adding given data to the data frame. The easiest example is 

    $df = $df->rbind([x1,...,xp]);

Then the last row of `$df` becomes `[x1,...,xp]`.

This method accepts a data frame and a hash as well.

    $df->rbind($dg);
    $df->rbind({col1=>x1,col2=>x2,...});

If a column name of `$df` can not be found in the column names of `$dg` (or keys
of the hash), then undef is used for the corresponding values.

### merge

Not yet implemented.

### rapply(\&func) and rapply_th(\&func)

This is a degenerate version of `apply(df,1,func)` in R: The function `func()` takes each array (not an arrayref) and must give a scalar
value. For example 

    $df->rapply(sub{ $_[0] + $_[1] });

gives an array consisting of the sum of the first and second elements of rows.

When we want to use the column names, we use `rapply_th()`. 

    $df->rapply_th(sub{ $_[0]{col0} + $_[0]{col1} })

Note that the `$_[0]` is the reference to the hash of a row.

### capply(\&func,$cols)

This is a similar function to `apply(df,2,func)` in R: This method apply the function reference to spcified columns. The function reference takes an array of a column and must give a scalar value.

    $df->capply(sub{ my $s; $s += $_ foreach @_; $s; }, "col0");

gives the sum of all elements of the column "col0". If we omit $cols, then \&func is applied to all columns.

Note that the order of `\&func` and `$cols`.

## Dealing with missing data

### complete_cases

Not yet.

## Converting a wide table to a long one and vice versa

### melt

not yet

### dcast

not yet

## methods corresponding to functions of dplyr in R

The methods which are explained in this section **create a new data frame**. 

### filter(\&func) and filter_th(\&func)

This method creates a new data subframe consisting of rows on which \&func gives true. The return value of \&func is used as a boolean value. For example

    $df->filter(sub{ $_[1] == 17 });

gives the data subframe consisting of the rows whose second element is equal to 17 and 

    $df->filter(sub{ $_[0]{'name'} =~ /honda/ });

gives the data subframe consisging of the rows whose values of "name" contain "honda".

### slice

This creates the data subframe consisting of the specified rows. For example

    $df->slice(0..9);

gives the data subframe consisting of the first 10 rows of $df. This method also accepts an arrayref.

### arrange

This methods arranges the rows. An easiest usage is 

    $df->arrange('year','month','day');

This gives a new data frame which is sorted by columns 'year', 'month' and 'day'. The default comparison is "<=>", i.e.

- The rows are sorted in the ascending order.
- The elemens of the specified column are assumed to be numeric.

To sort the rows in descending order, put `["col1",desc=>1]` instead of "col1". To use "cmp" for comparison, put `["col1",cmp=>]` intead. The value of cmp accepts also a function refernce. For example, 

    $df->arrange(["name",desc=>1,cmp=>sub {$_[0] cmp $_[1]}]);

is equivalent to 

    $df->arrange(["name",desc=>1,cmp=>1]);

### select

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

### rename(new_name0=>old_name0,new_name1=>old_name1,...)

This change the column names. Note that the new names are the keys of the argument of the method. If a specified old name does not belong to the column names, then it is ignored.

Use `set_name()` method, when modifying column names to remove invalid column names. The `rename()` method assumes that there is no invalid column name.

### distinct 

This method is quite different from distinct of dplyr, but similar to DISTINCT of SQL. For example

    $df->distinct("col0","col2");

consists of two columns and its rows are distinct values of the two columns. The above line is equivalent to `distinct(select(df,col0,col2))` in R.

### mutate() and mutate_th()

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

### transmute() and transmute_th()

Not yet implemented. Use mutate() and select().

### summarise() 

This method produce a data frame consisting of a single row. In an example

    $df->summarise( ['ave', \&func, 'col2'] );

we apply an aggregate function \&func to the colunm 'col2', then its returned value is the unique value of the column 'ave' of the new data frame. Thus the above line is equivalent to 

    Arrow::DataFrame->new(rows=>[[$df->capply(\&func,'col2')]],names=>['ave']);

We can give several triples:

    $df->summarise( ['ave0', \&func0, 'col0'] , ['ave1', \&func1, 'col1'], ...);

### groupby

Not yet implemented.

# TODO

- More functions! 
- read_csv should accept a URL and deal with an NA character.
- The output of del_cols (and del_rows) should be a data frame or a hash
- read_csv should accept a reference to convert elements of a column.
- to_json, read_json (so that they are compatible with the functions corresponding to R )
- Make this independent of Arithmetic.pm.
- Consider indices of rows.
- use PDL

# AUTHOR

stdiff &lt;hsakai@stdiff.net>

# COPYRIGHT AND LICENSE

This software is copyright (c) 2015 by stdiff.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.
