# crossmatch benchmark - delucchi fork

I tried to run Kostya's benchmarking routines, on epyc. This was to expand the timing points to the left, and fill in some other points to the right. I had to make several changes to make it work on epyc, and to remove some of the flexibility (because I'm a jerk).

## Setup

You have to install smatch from source. Don't try to install from pip! It's not the same smatch!

```
git clone http://github.com/esheldon/smatch
```

