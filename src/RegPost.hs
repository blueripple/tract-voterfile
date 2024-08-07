{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module RegPost where

import Data.String.Here (here)

a1 :: Text
a1 = [here|
# Voter Registration in Pennsylvania
At BlueRipple we have been working to analyze registration, turnout, and voter preference in smaller
geographies. We've looked at congressional districts each with about 700,000 people,
then state-legislative districts (SLDs) ranging from 20,000 to 100,000 people in each. Now we're
beginning to look at census-tracts, with 2,000 to 6,000 people in each. Being able to "zoom in" this way
gives us information about what is happening at the scale of neighborhoods instead of whole districts.

As we began this work we discovered Redistricting Data Hub ([RDH](https://redistrictingdatahub.org/)). RDH offers
census-block-level aggregations of the national voter file[^voterfile] for non-partisan use.
This data is provided to RDH by [L2](https://www.l2-data.com/) which acquires voter files from each state,
cleans that data and makes uniformly processed versions avaliable to RDH for use in non-partisan work,
particularly on redistricting issues. This data-set provides the number of registered voters and the
number of them who voted (as well as various demographic
breakdowns) for each census-block. Census-blocks are easily aggregated to census-tracts, giving us real numbers to compare to our
models or use in conjunction with them.

[^voterfile]: Each state maintains a database of registered voters. This includes addresses and a record of whether they voted in
some number of past primaries and general elections. Each state has its own format for this data and its own procedure and
fees for access. Various companies offer harmonized versions of that data for various users, primarily campaigns and PACs which use
it to target their advertising. The version we get from RDH has no addresses or individually identifying data.

BlueRipple normally focuses our analyses on things particularly
helpful to Democratic donors and candidates.
Nonetheless, we have a sincere non-partisan interest in voter-registration and voter turnout.
We believe that more voters leads to better functioning democracy.
So in what follows, we examine voter registration in Pennsylvania (PA) and see what we can learn
with use of this newly available data and our ability to model in smaller geographies.

PA is widely seen as the tipping point[^tipping] state in this election which makes it
an especially interesting place to examine. According to the Census Bureau's 2022 CPS
survey, about 72% of the citizen voting-age population (CVAP) is registered. On the one hand, that leaves a lot of potential
voters to register (almost 3 million people!) and on the other hand, that's fairly typical of the rest of the country. So, if you want
to register more voters in PA, what should you do? Looking at actual and modeled registration in each census tract gives some hints.

[^tipping]: If the election is decided by just one state, PA is, according to a few election models, most likely to be that
state. This is determined by running many simulations of the election with probabilities and correlations determined by polling
averages and various fundamental indicators. In a given simulation, suppose candidate A wins the election. Put the states in descending
order by the winning margin of candidate A. As you go through that list accumulating electoral votes for candidate A, there will be
a state which "tips" the total over to majority for candidate A.
The most frequent such state in all runs of the simulation is the tipping point state.
See [https://en.wikipedia.org/wiki/Tipping-point_state](https://en.wikipedia.org/wiki/Tipping-point_state).


### Contents
1. [Modeling Voter Registration In Census Tracts](#s1)
2. [PA Registration: Statewide Demographics](#s2)
3. [PA Registration: Census Tracts](#s3)

### Modeling Voter Registration In Census Tracts {#s1}

## Why Model Voter Registration?
Since we know the exact number of registered voters from the voter-file data,
why do we bother to model it? There are a few reasons:

- Demographic patterns of low and high registration. These might be useful for targeting registration drives.
E.g., young people are less likely to register to vote. Perhaps knowing how that depends on education, race, or location can help
us target registration efforts more effectively.
- Differences between the model and the observed registration. Places where registration
is lower than the model expectation might be particularly good places to do registration work.

Imagine you want to target voter registration efforts in places with low registration. This could mean anyplace where
registration rates are significantly below the state or national average. But that's a lot of places! It might be more useful to start
with places where registration is not just lower than average but also lower than expected. For example, people under 35
have much lower registration rates than people over 35. So census-tracts with predominantly younger people (think college-campuses)
will often have very low registration rates. And while a registration drive in such a place might be useful, it might be
more productive to go someplace with low registration and people who are more likely to be
registered. To find those places we need both the voter-file data, and a model of voter registration for comparison.

## How BlueRipple Models Voter Registration
In the technical appendix to a [previous post](https://blueripple.github.io/research/Election/ModelNotes/post.html) about modeling
turnout and voter preference, we describe our approach in some detail. The short version: we use the Cooperative Election Survey (CES)
as a source for modeling registration, partisan Id, turnout, and presidential vote. Our model predicts those things as
a function of age, sex, education, race/ethnicity, state and population density. In order to apply the model to predict something in a specific
geography, we need to know how many citizens of voting age of each type (e.g., 35-45 year old Asian women with college degrees) live there.
Our source for this data is the Census Bureau's American Community Survey[^acsIssues].

[^acsIssues]: At the census-tract level, the ACS does not actually give us the breakdown, also known as a "joint distribution",
we need, which we refer to as CASER (*C*itizenship *A*ge *S*ex *E*ducation *R*ace/Ethnicity)  Instead it provides several smaller joint
distributions (CSR, ASR and ASE). We have good data for the full CASER table at larger geographies, known as
Public Use Microdata Areas (or PUMAs). We use a model, informed by the PUMAs, of the relationship of the joint table to the smaller ones
in order to combine the smaller tables in a way which preserves all the information
in them while incorporating information from the full tables at larger geographic scales.

In that post we were concerned with modeling voter turnout and presidential vote. In this post we will be focused on voter registration.
The CES survey also asks questions about voter registration and validates those answers via a voter-file. So the modeling task is
almost exactly the same as modeling voter turnout. As in our turnout model, given a person in a specific
demographic category (one of 200 possible combinations of age, sex, education and race/ethnicity), in a state and living
in a place with a particular population density, we estimate a probability that such a person is registered to vote.

With that model in hand, we construct the joint distribution of people in the census tract, along with the population density.
That allows us to estimate the probability that each person is registered to vote.
We add those probabilities up across all the people and that gives us an estimated number of registered voters of each type in each tract.

### PA Registration: Statewide Demographics
Let's take a quick look at the demographics of voter registration across the state. Since we've mentioned the variations in
registration rate with age, We'll start there. Age is also a variable which is tracked in the voter-files so we can compare the model
to the actual numbers. In the table below we see that the model tracks the general trend of increasing rate as age goes up but misses
the truth in PA in various ways. Most noticeable is that the model expects much lower registration in the 25 to 34 than we see in PA and
much higher registration in the 45 to 64 age group[^diffs].

[^diffs]: Differences between the model and the actual numbers can arise for several reasons. Our sample in PA might not be representative and/or
PA might differ enough from the rest of the country to throw off the model, which balances PA data with national data. One tricky aspect
is that the voter-file contains data for registered voters who have moved or died but that info hasn't made it to the file yet. Our
survey data is based on interviews so it does not suffer from this problem. It's also possible that our estimates of the CVAP are not quite right.
We see evidence of that in the voter file where the CVAP we get from the ACS is lower than the number of registered voters!

We think the model is a reasonable proxy for expected registration. And thus PA's high registration among 25 to 34 year-olds is to be
commended while the surprisingly low registration among 45 to 64 year olds is something that might be addressed with a targeted registration
drive.

Suppose we wanted to know more specifically who to target in a registration drive among 45 to 64 year olds? Now we can only use the model,
since the voter-file doesn't contain any breakdowns with multiple categories. Let's look at the sex, education and race/ethnicity breakdown
of modeled registration rates in PA.




|]
