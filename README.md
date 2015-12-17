# DataFrame.pm

## Description

This Perl module provides a degenerated variant of a data frame in R. The names of methods follow mostly ones in R. Only very basic functions are currently implemented.

- Please read `pod-DataFrame.md` at first. You can also browse it by perldoc:

		$ perldoc Arrow/DataFrame.pm

  Or [http://stdiff.net/?pod-DataFrame](http://stdiff.net/?pod-DataFrame).
- You can get an overview by executing the perl script `test-DataFrame.pl`. This script downloads a well-known [Auto.csv](http://www-bcf.usc.edu/~gareth/ISL/data.html) and shows how to use DataFrame.pm.
- Several methods are not explained in the documentation. Please avoid using such a method, because it will substantially be changed.
- At your own risk.
- Your feedback is always welcome.

## How to use

1\. Create `Arrow` directory and put `DataFrame.pm` into it.

	.
	├── Arrow
	│   └── DataFrame.pm
	└── YOUR_PERL_SCRIPT.pl

2\. Add the following line to your script.

	use Arrow::DataFrame;

## ChangeLog

- 2015/12/17 : In Internet.
