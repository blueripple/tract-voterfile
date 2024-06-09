{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3
import qualified BlueRipple.Data.Small.Loaders as BRL
import qualified BlueRipple.Data.Small.DataFrames as BR
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Modeling as MT
--import qualified BlueRipple.Data.CES as CCES
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.ACS_Tables_Loaders as BRC
import qualified BlueRipple.Data.ACS_Tables as BRC
import qualified BlueRipple.Data.RDH_Voterfiles as VF
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs

import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.DesignMatrix as DM

import qualified Frames as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.CSV as FCSV

import Frames.Streamly.Streaming.Streamly (StreamlyStream, Stream)

import qualified Control.Foldl as FL
import Control.Lens (view, (^.))

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Functor as V

import qualified Text.Printf as PF
import qualified System.Environment as Env

import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ


import Path (Dir, Rel)
import qualified Path

templateVars ∷ Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "../../research/pandoc-templates/blueripple_basic.html"

dmr ::  DM.DesignMatrixRow (F.Record DP.LPredictorsR)
dmr = MC.tDesignMatrixRow_d

survey :: MC.TurnoutSurvey (F.Record DP.CESByCDR)
survey = MC.CESSurvey

aggregation :: MC.SurveyAggregation TE.ECVec
aggregation = MC.WeightedAggregation MC.ContinuousBinomial

alphaModel :: MC.Alphas
alphaModel = MC.St_A_S_E_R_StR  --MC.St_A_S_E_R_AE_AR_ER_StR

--type SLDKeyR = '[GT.StateAbbreviation] V.++ BRC.LDLocationR
type ModeledR = BRC.TractLocationR V.++ '[MR.ModelCI]

main :: IO ()
main = do
  cmdLine ← CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig ←
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BR.brWriterOptionsF . K.mindocOptionsF)
  cacheDir <- toText . fromMaybe ".kh-cache" <$> Env.lookupEnv("BR_CACHE_DIR")
  let knitConfig ∷ K.KnitConfig BRCC.SerializerC BRCC.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "Tract-VoterFile"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRCC.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    exploreTractVoterfile cmdLine postInfo "PA"
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err


exploreTractVoterfile :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r)
                      => BR.CommandLine
                      -> BR.PostInfo
                      -> Text
                      -> K.Sem r ()
exploreTractVoterfile cmdLine pi sa = do
  postPaths <- postPaths "TractTest" cmdLine
  BRK.brNewPost postPaths pi "TractTest" $ do
    vfByTract_C <- VF.voterfileByTracts Nothing
    states <- K.ignoreCacheTimeM BRL.stateAbbrCrosswalkLoader
    let fipsByState = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. GT.stateFIPS)) FL.map) states
    stateFIPS <- K.knitMaybe ("FIPS lookup failed for " <> sa) $ M.lookup sa fipsByState
    vfByTract <- K.ignoreCacheTime vfByTract_C

    K.logLE K.Info $ "Voter Files By Tract has " <> show (length vfByTract) <> " rows."
    let vfByTractForState = F.filterFrame ((== stateFIPS) . view GT.stateFIPS) vfByTract
    K.logLE K.Info $ sa <> " voter Files By Tract has " <> show (length vfByTractForState) <> " rows."

--    (F.takeRows 100 <$> K.ignoreCacheTime vfByTract_C) >>= BRLC.logFrame
    let geoid r = let x = r ^. GT.tractGeoId in if x < 10000000000 then "0" <> show x else show x
        regDiff r =
          let ds = realToFrac (r ^. VF.vFPartyDem)
              rs = realToFrac (r ^. VF.vFPartyRep)
          in ds - rs --if ds + rs > 0 then (ds - rs) / (ds + rs) else 0
    geoExample postPaths pi "geoExample" "Test" (FV.fixedSizeVC 1000 1000 10)
      ("/Users/adam/BlueRipple/bigData/GeoJSON/states/" <> sa <> "_2022_tracts_topo.json","tracts")
      geoid
      (regDiff, "RegDiff")
      vfByTractForState
      >>= K.addHvega Nothing Nothing
    pure ()

geoExample :: (K.KnitEffects r, Foldable f)
           => BR.PostPaths Path.Abs
           -> BR.PostInfo
           -> Text
           -> Text
           -> FV.ViewConfig
           -> (Text, Text)
           -> (row -> Text)
           -> (row -> Double, Text)
           -> f row
           -> K.Sem r GV.VegaLite
geoExample pp pi chartID title vc (geoJsonPath, topoJsonFeatureKey) geoid (val, valName) rows = do
  let colData r = [ ("GeoId", GV.Str $ geoid r)
                  , (valName, GV.Number $ val r)
                  ]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) rows
  jsonFilePrefix <- K.getNextUnusedId $ ("2023-StateLeg_" <> chartID)
  jsonDataUrl <-  BRK.brAddJSON pp pi jsonFilePrefix jsonRows
  geoJsonSrc <- K.liftKnit @IO $ Path.parseAbsFile $ toString geoJsonPath
  jsonGeoUrl <- BRK.brCopyDataForPost pp pi BRK.LeaveExisting geoJsonSrc Nothing
  let rowData = GV.dataFromUrl jsonDataUrl [GV.JSON "values"]
      geoData = GV.dataFromUrl jsonGeoUrl [GV.TopojsonFeature topoJsonFeatureKey]
      tLookup = GV.lookup "properties.geoid" rowData "GeoId" (GV.LuFields ["GeoId",valName])
      perValName = valName <> "perSqm"
      tComputePer = GV.calculateAs ("datum." <> valName <> "/ log(datum.properties.aland)") perValName
      transform = (GV.transform . tLookup . tComputePer) []
      encValPer = GV.color [GV.MName perValName, GV.MmType GV.Quantitative]
      encoding = (GV.encoding . encValPer) []
      mark = GV.mark GV.Geoshape []
      projection = GV.projection [GV.PrType GV.Identity, GV.PrReflectY True]
  pure $ BR.brConfiguredVegaLite vc [FV.title title, geoData, transform, projection, encoding, mark]

{-
modelWhiteEvangelicals :: (K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> K.Sem r ()
modelWhiteEvangelicals cmdLine = do
  let psName = "GivenWWH"
      psType = RM.PSGiven "E" psName ((`elem` [DT.R5_WhiteNonHispanic, DT.R5_Hispanic]) . view DT.race5C)
      cacheStructure cy = MR.CacheStructure (Right $ "model/evangelical/stan/CES" <> show (CCES.cesYear cy)) (Right "model/evangelical")
                          psName () ()
      modelConfig am = RM.ModelConfig aggregation am (contramap F.rcast dmr)
      modeledToCSVFrame = F.toFrame . fmap (\(k, v) -> k F.<+> FT.recordSingleton @MR.ModelCI v) . M.toList . MC.unPSMap . fst
  modeledACSBySLDPSData_C <- modeledACSBySLD cmdLine BRC.TY2021
--    districtPSData <- K.ignoreCacheTime modeledACSBySLDPSData_C
  let dBDInnerF :: FL.Fold (F.Record '[DT.Race5C, DT.PopCount]) (F.Record [DT.PopCount, WhiteVAP])
      dBDInnerF =
        let pop = view DT.popCount
            race = view DT.race5C
            isWhite = (== DT.R5_WhiteNonHispanic) . race
            popF = FL.premap pop FL.sum
            whiteF = FL.prefilter isWhite popF
        in (\p w -> p F.&: w F.&: V.RNil) <$> popF <*> whiteF
      dataByDistrictF = FMR.concatFold
                        $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @SLDKeyR @[DT.Race5C, DT.PopCount])
                        (FMR.foldAndAddKey dBDInnerF)
  dataByDistrict <-  fmap (FL.fold dataByDistrictF . DP.unPSData) $ K.ignoreCacheTime modeledACSBySLDPSData_C

  let addDistrictData :: K.KnitEffects r
                      =>  F.FrameRec (SLDKeyR V.++ '[MR.ModelCI])
                      -> K.Sem r (F.FrameRec (SLDKeyR V.++ [MR.ModelCI, DT.PopCount, WhiteVAP, WhiteEv]))
      addDistrictData x =  do
        let (joined, missing) = FJ.leftJoinWithMissing @SLDKeyR x dataByDistrict
        when (not $ null missing) $ K.logLE K.Error $ "Missing keys in result/district data join=" <> show missing
        let addEv r = r F.<+> FT.recordSingleton @WhiteEv (round $ MT.ciMid (r ^. MR.modelCI) * realToFrac (r ^. DT.popCount))
        pure $ fmap addEv joined
--    modeledEvangelical_C <- RM.runEvangelicalModel @SLDKeyR CCES.CES2020 (cacheStructure CCES.CES2020) psType (modelConfig MC.St_A_S_E_R) modeledACSBySLDPSData_C
--    modeledEvangelical_AR_C <- RM.runEvangelicalModel @SLDKeyR CCES.CES2020 (cacheStructure CCES.CES2020) psType (modelConfig MC.St_A_S_E_R_AR) modeledACSBySLDPSData_C
--    modeledEvangelical_StA_C <- RM.runEvangelicalModel @SLDKeyR CCES.CES2020 (cacheStructure CCES.CES2020) psType (modelConfig MC.St_A_S_E_R_StA) modeledACSBySLDPSData_C
  modeledEvangelical22_StR_C <- RM.runEvangelicalModel @SLDKeyR CCES.CES2022 (cacheStructure CCES.CES2022) psType (modelConfig MC.St_A_S_E_R_StR) modeledACSBySLDPSData_C
--    modeledEvangelical20_StR_C <- RM.runEvangelicalModel @SLDKeyR CCES.CES2020 (cacheStructure CCES.CES2020) psType (modelConfig MC.St_A_S_E_R_StR) modeledACSBySLDPSData_C
  let compareOn f x y = compare (f x) (f y)
      compareRows x y = compareOn (view GT.stateAbbreviation) x y
                        <> compareOn (view GT.districtTypeC) x y
                        <> GT.districtNameCompare (x ^. GT.districtName) (y ^. GT.districtName)
      csvSort = F.toFrame . sortBy compareRows . FL.fold FL.list
--    modeledEvangelical <-
--    K.ignoreCacheTime modeledEvangelical_C >>= writeModeled "modeledEvangelical_GivenWWH" . csvSort . fmap F.rcast . modeledToCSVFrame
--    K.ignoreCacheTime modeledEvangelical_AR_C >>= writeModeled "modeledEvangelical_AR_GivenWWH" . csvSort . fmap F.rcast . modeledToCSVFrame
--    K.ignoreCacheTime modeledEvangelical_StA_C >>= writeModeled "modeledEvangelical_StA_GivenWWH" . csvSort . fmap F.rcast . modeledToCSVFrame
  K.ignoreCacheTime modeledEvangelical22_StR_C
    >>= fmap (fmap addTSPId) . addDistrictData . csvSort . fmap F.rcast . modeledToCSVFrame
    >>= writeModeled "modeledEvangelical22_NLCD_StR_GivenWWH" . fmap F.rcast
--    K.ignoreCacheTime modeledEvangelical20_StR_C >>= writeModeled "modeledEvangelical20_StR_GivenWWH" . csvSort . fmap F.rcast . modeledToCSVFrame
--    let modeledEvangelicalFrame = modeledToCSVFrame modeledEvangelical
--    writeModeled "modeledEvangelical_StA_GivenWWH" $ fmap F.rcast modeledEvangelicalFrame
--    K.logLE K.Info $ show $ MC.unPSMap $ fst $ modeledEvangelical

-}

{-
modeledACSBySLD :: forall r . (K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> BRC.TableYear -> K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD cmdLine ty = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  (jointFromMarginalPredictorCSR_ASR_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                              >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                              (Right "CSR_ASR_ByPUMA")
                                              (Right "model/demographic/csr_asr_PUMA")
                                              (DTM3.Model $ tsModelConfig "CSR_ASR_ByPUMA" 71) -- use model not just mean
                                              Nothing Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                               >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                               (Right "CASR_SER_ByPUMA")
                                               (Right "model/demographic/casr_ase_PUMA")
                                               (DTM3.Model $ tsModelConfig "CASR_ASE_ByPUMA" 141) -- use model not just mean
                                               Nothing Nothing Nothing . fmap (fmap F.rcast)
  (acsCASERBySLD, _products) <- BRC.censusTablesForSLDs 2024 ty
                                >>= DMC.predictedCensusCASER' (DTP.viaOptimalWeights DTP.euclideanFull) (Right "model/election2/sldDemographics")
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRCC.retrieveOrMakeD ("model/election2/data/sldPSData" <> BRC.yearsText 2024 ty <> ".bin") acsCASERBySLD
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)


type ASRR = '[BR.Year, GT.StateAbbreviation] V.++ BRC.TractLocationR V.++ [DT.Age6C, DT.SexC, BRC.RaceEthnicityC, DT.PopCount]
-}
{-
givenASRBySLD :: (K.KnitEffects r, BRCC.CacheEffects r) => BRC.TableYear -> K.Sem r (F.FrameRec ASRR)
givenASRBySLD ty = do
  asr <- BRC.ageSexRace <$> (K.ignoreCacheTimeM $ BRC.censusTablesForSLDs 2024 ty)
  asr' <- BRL.addStateAbbrUsingFIPS asr
  pure $ fmap F.rcast asr'
-}
postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|tract-voterfile/posts|]

postLocalDraft
  ∷ Path.Path Rel Dir
  → Maybe (Path.Path Rel Dir)
  → Path.Path Rel Dir
postLocalDraft p mRSD = case mRSD of
  Nothing → postDir BR.</> p BR.</> [Path.reldir|draft|]
  Just rsd → postDir BR.</> p BR.</> rsd

postInputs ∷ Path.Path Rel Dir → Path.Path Rel Dir
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]

sharedInputs ∷ Path.Path Rel Dir
sharedInputs = postDir BR.</> [Path.reldir|Shared|] BR.</> [Path.reldir|inputs|]

postOnline ∷ Path.Path Rel t → Path.Path Rel t
postOnline p = [Path.reldir|research/TractVoterFile|] BR.</> p

postPaths
  ∷ (K.KnitEffects r)
  ⇒ Text
  → BR.CommandLine
  → K.Sem r (BR.PostPaths BR.Abs)
postPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ → maybe Nothing BR.parseRelDir $ fmap toString mS
        _ → Nothing
  postSpecificP ← K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnline postSpecificP)
