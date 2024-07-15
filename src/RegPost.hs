{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module RegPost where

import Data.String.Here (here)

intro :: Text
intro = [here|
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

BlueRipple normally focuses our analyses on things particularly
helpful to Democratic donors and candidates.
Still, we have a sincerely *non-partisan* interest in voter-registration.
We believe that more people voting leads to a better functioning democracy. That makes registration a perfect place to focus our
use of this newly available data and our ability to model in smaller geographies.

Pennsylvania (PA) is widely seen as the tipping point[^tipping] state in this election. According to the Census Bureau's 2022 CPS
survey, about 72% of the citizen voting-age population (CVAP) is registered. On the one hand, that leaves a lot of potential
voters to register (almost 3 million people!) and on the other hand, that's fairly typical of the rest of the country. So, if you want
to register more voters in PA, what should you do? Looking at actual and modeled registration in each census tract gives some hints.

[^voterfile]: Each state maintains a database of registered voters. This includes addresses and a record of whether they voted in
some number of past primaries and general elections. Each state has its own format for this data and its own procedure and
fees for access. Various companies offer harmonized versions of that data for various users, primarily campaigns and PACs which use
it to target their advertising. The version we get from RDH has no addresses or individually identifying data.

[tipping^]: The tipping point state is the one which is most likely to be the closest in an election decided by one state. In
other words, if the election is decided by just one state, PA is, according to a few election models, most likely to be that
state.

### Contents
1. [Modeling Voter Registration In Census Tracts](#s1)
2. [Census Tract Demographics](#s2)
3. [PA Registration By the Numbers]

### Modeling Voter Registration In Census Tracts {#s1}

## Why Model Voter Registration?
Since we know the exact number of registered voters from the voter-file data,
why do we bother to model it? There are a few reasons:

- We're interested in demographic patterns of low and high registration that might be useful for targeting registration drives.
Young people are less likely to be register to vote. Perhaps knowing how that depends on education, race, or location can help
us target registration efforts more effectively.
- Differences between the model and the observed registration, especially places where registration
is lower than the model expectation might be particularly good places to do registration work.

Imagine you want to target voter registration efforts in places with low registration. This could mean anyplace where
registration rates are significantly below the state or national average. But that's a lot of places! It might be more useful to start
with places where registration is not just lower than average but also lower than expected. For example, people under 35
have much lower registration rates than people over 35. So census-tracts with predominantly younger people (think college-campuses)
will often have very low registration rates. And while a registration drive in such a place might be useful, it might be
more productive to go someplace with low registration and people who are usually likely to be
registered. To find those places we need both the voter-file data, and a model of voter registration for comparison.

## How BlueRipple Models Voter Registration
In the technical appendix to a [previous post](https://blueripple.github.io/research/Election/ModelNotes/post.html) about modeling
turnout and voter preference, we describe our approach in some detail. The short version: we use the Cooperative Election Survey (CES)
as a source for modeling registration, partisan Id, turnout, and presidential vote. Our model predicts those things as
a function of age, sex, education, race/ethnicity, state and population density. In order to apply the model to predict something in a specific
geography, we need to know how many citizens of voting age of each type (e.g., 35-45 year old Asian women with college degrees) live there.
Our source for this data is the Census Bureau's American Community Survey[^acsIssues].

[^acsIssues]: At the census-tract level, the ACS does not actually give us the breakdown, also known as a "joint distribution",
we need, which we refer to as *CASER* (Citizenship Age Sex Education Race/Ethnicity)  Instead it provides several smaller joint
distributions (in particular, CSR, ASR and ASE). We have good data for the full CASER table at larger geographies, known as
Public Use Microdata Areas (or PUMAs). We use a model to combine the smaller tables in a way which preserves all the information
in them while incorporating information from the full tables at larger geographic scales.

In that post we were concerned with modeling voter turnout and presidential vote. In this post we will be focused on voter registration.
The CES survey also asks questions about voter registration and validates those answers via a voter-file. So the modeling task is
almost exactly the same as modeling voter turnout. As in our turnout model, given a person in a specific
demographic category (one of 200 possible combinations of age, sex, education and race/ethnicity), in a state and living
in a place with a particular population density, we estimate a probability that such a person is registered to vote.

With that model in hand, we construct the joint distribution of people in the census tract, along with the population density.
That allows us to estimate the probability that each person is registered to vote.
We add those probabilities up across all the people and that gives us an estimated number of registered voters.

|]
