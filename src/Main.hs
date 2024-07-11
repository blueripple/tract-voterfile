{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
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
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3
import qualified BlueRipple.Data.Small.Loaders as BRL
import qualified BlueRipple.Data.Small.DataFrames as BR
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified BlueRipple.Data.Small.DataFrames as BRDF
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
import qualified Frames.Streamly.InCore as FSI

import Frames.Streamly.Streaming.Streamly (StreamlyStream, Stream)

import qualified Control.Foldl as FL
import Control.Lens (view, (^.))

import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
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
import GHC.TypeLits (Symbol)

import Path (Dir, Rel)
import qualified Path
import qualified Frames.Folds as FF

FS.declareColumn "Registered" ''Int
FS.declareColumn "Voted20"      ''Int
FS.declareColumn "Voted22"      ''Int
FS.declareColumn "RegistrationP" ''MT.ConfidenceInterval
FS.declareColumn "TurnoutP" ''MT.ConfidenceInterval
FS.declareColumn "DemIdP" ''MT.ConfidenceInterval

FS.declareColumn "CitPop" ''Int
FS.declareColumn "AllPop" ''Int
FS.declareColumn "ADV" '' Double
FS.declareColumn "RR" '' Double
FS.declareColumn "MRR" '' Double
FS.declareColumn "RGap" '' Double
FS.declareColumn "MRGap" '' Double


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

type TractGeoR = [BRDF.Year, GT.StateAbbreviation, GT.TractGeoId]

dmr ::  DM.DesignMatrixRow (F.Record DP.LPredictorsR)
dmr = MC.tDesignMatrixRow_d

survey :: MC.ActionSurvey (F.Record DP.CESByCDR)
survey = MC.CESSurvey

aggregation :: MC.SurveyAggregation TE.ECVec
aggregation = MC.WeightedAggregation MC.ContinuousBinomial

alphaModel :: MC.Alphas
alphaModel = MC.St_A_S_E_R_StR  --MC.St_A_S_E_R_AE_AR_ER_StR

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
--          , K.lcSeverity = M.fromList [("KH_Cache", K.Special), ("KH_Serialize", K.Special), ("OptimalWeights", K.Special)]
--          , K.lcSeverity = M.fromList [("OptimalWeights", K.Special)]
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRCC.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
--    exploreTractVoterfile cmdLine postInfo "PA"
--    regPAMap_C <- modeledRegistrationForState cmdLine "PA"
--    K.ignoreCacheTime regPAMap_C >>= putTextLn . show . MC.unPSMap
    vfAndModeledPA_C <- vfAndModeledByState cmdLine "PA"
    K.ignoreCacheTime vfAndModeledPA_C >>= writeVFAndModeledToCSV "PA/PA"
--    compareCVAPs cmdLine "GA"
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err


writeVFAndModeledToCSV :: K.KnitEffects r => Text -> F.FrameRec VFAndModeledR -> K.Sem r ()
writeVFAndModeledToCSV csvName f = do
  let adv r = realToFrac (r ^. registered - r ^. voted20) * (MT.ciMid (r ^. demIdP) - 0.5)
      rr r = realToFrac (r ^. registered) / realToFrac (r ^. DT.popCount)
      mrr r =  MT.ciMid (r ^. registrationP) --realToFrac (r ^. registered) / realToFrac (r ^. DT.popCount) / MT.ciMid (r ^. registrationP)
      pop = realToFrac . view DT.popCount
      avgRRFld = (/) <$> FL.premap (\r -> pop r * rr r) FL.sum <*> FL.premap pop FL.sum
      avfMRRFld = (/) <$> FL.premap (\r -> pop r * mrr r) FL.sum <*> FL.premap pop FL.sum
      adjFld = (/) <$> avgRRFld <*> avfMRRFld
      (adj, avgRR) = FL.fold ((,) <$> adjFld <*> avgRRFld) f
      advC = FT.recordSingleton @ADV . adv
      rrC = FT.recordSingleton @RR . rr
      rGapC r = FT.recordSingleton @RGap $ (rr r - avgRR) * pop r
      mrrC r = FT.recordSingleton @MRR $ adj * mrr r
      mrGapC r = FT.recordSingleton @MRGap $ (rr r - adj * mrr r) * pop r
      addCols r = r  F.<+> rrC r F.<+> rGapC r F.<+> mrrC r F.<+> mrGapC r F.<+> advC r
  let wText = FCSV.formatTextAsIs
      printNum n m = PF.printf ("%" <> show n <> "." <> show m <> "g")
      wPrintf :: (V.KnownField t, V.Snd t ~ Double) => Int -> Int -> V.Lift (->) V.ElField (V.Const Text) t
      wPrintf n m = FCSV.liftFieldFormatter $ toText @String . printNum n m
      wCI :: (V.KnownField t, V.Snd t ~ MT.ConfidenceInterval) => Int -> Int -> V.Lift (->) V.ElField (V.Const Text) t
      wCI n m = FCSV.liftFieldFormatter
                $ toText @String .
                \ci -> printNum n m (100 * MT.ciMid ci)

      format = FCSV.formatTextAsIs V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow
               V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow
               V.:& wCI 2 2 V.:& wCI 2 2 V.:& wCI 2 2
               V.:& wPrintf 2 2 V.:& wPrintf 2 2 V.:& wPrintf 2 2 V.:& wPrintf 2 2 V.:& wPrintf 2 2 V.:& V.RNil
      newHeaderMap = M.fromList [("StateAbbreviation", "State")]

  K.liftKnit @IO
    $ FCSV.writeLines (toString $ "../../ModeledTracts/" <> csvName <> ".csv")
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap format ","
    $ FCSV.foldableToStream (fmap addCols f)


type VFAndModeledR = [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount, Registered, Voted20, Voted22, RegistrationP, TurnoutP, DemIdP]

vfAndModeledByState :: (K.KnitEffects r, BRCC.CacheEffects r)
                       =>  BR.CommandLine -> Text -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec VFAndModeledR))
vfAndModeledByState cmdLine sa = do
   let geoid r = let x = r ^. GT.tractGeoId in if x < 10000000000 then "0" <> show x else show x
       filterByState :: (FC.ElemsOf rs '[GT.StateAbbreviation], FSI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
       filterByState = F.filterFrame ((== sa) . view GT.stateAbbreviation)
       lt = K.logTiming (K.logLE K.Info)
--   K.logLE K.Info "Modeled ACS Data"
   K.logLE K.Info "Aggregate ACS Data for CVAPs"
   modeledACSByTractPSData_C <- modeledACSByTract cmdLine BRC.TY2022
   cvapCounts_C <- fmap filterByState
                   <$> BRCC.retrieveOrMakeFrame "analysis/tract-voterfile/tractCVAPs.bin" modeledACSByTractPSData_C (lt "tractCVAPs" . pure . tractCVAPs)
   K.logLE K.Info "load, filter and summarize voterfile data"
   vfByTract_C <- fmap (vfRegVoted . filterByState) <$> VF.voterfileByTracts Nothing
   K.logLE K.Info "Model registration"
   regForState_C <- modeledRegistrationForState cmdLine sa
   K.logLE K.Info "Model turnout"
   turnoutForState_C <- modeledTurnoutForState cmdLine sa
   K.logLE K.Info "Model partisan Id"
   demIdForState_C <- modeledPartisanIdOfRegForState cmdLine sa
   let modeledRegTurnoutIdCK = "analysis/tract-voterfile/" <> sa <> "/modeledRegTurnoutId.bin"
       regTurnoutIdDeps = (,,) <$> regForState_C <*> turnoutForState_C <*> demIdForState_C
   K.logLE K.Info "Merge models"
   regTurnoutIdByTractForState_C <- BRCC.retrieveOrMakeFrame modeledRegTurnoutIdCK regTurnoutIdDeps
                                (lt "modelMerge" . K.knitEither . \(r, t, i) -> mergeRegTurnoutId r t i)

   let joinDeps = (,,) <$> cvapCounts_C <*> vfByTract_C <*> regTurnoutIdByTractForState_C
       vfAndModeledCK = "analysis/tract-voterfile/" <> sa <> "/vfAndModeled.bin"
   K.logLE K.Info "Join..."
   BRCC.retrieveOrMakeFrame vfAndModeledCK joinDeps (lt "CVAP + VF + Model join" . K.knitEither . \(c, v, m) -> joinCVAP_VF_Model c v m)
--   (joined, missingVFCount, missingVFCount_model) = FJ.leftJoin3WithMissing @BRC.TractLocationR vfByTract_C


joinCVAP_VF_Model :: F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount]
                  -> F.FrameRec (BRC.TractLocationR V.++ [Registered, Voted20, Voted22])
                  -> F.FrameRec (BRC.TractLocationR V.++ [RegistrationP, TurnoutP, DemIdP])
                  -> Either Text (F.FrameRec VFAndModeledR)
joinCVAP_VF_Model cvap vf model = do
  let (joined, missingCVAP_VF, missingModel) = FJ.leftJoin3WithMissing @BRC.TractLocationR cvap vf model
  when (not $ null missingCVAP_VF) $ Left $ "Missing keys in CVAP/VF join: " <> show missingCVAP_VF
  when (not $ null missingModel) $ Left $ "Missing keys in CVAP+VF/Model join: " <> show missingModel
  pure joined

vfRegVoted :: F.Frame VF.VF_Raw -> F.FrameRec (BRC.TractLocationR V.++ [Registered, Voted20, Voted22])
vfRegVoted = fmap (F.rcast . FT.mutate f)
  where
    totalReg r = FT.recordSingleton @Registered $ r ^. VF.partyDem + r ^. VF.partyRep + r ^. VF.partyOth
    totalVoted20 r = FT.recordSingleton @Voted20 $ r ^. VF.g20201103VotedAll
    totalVoted22 r = FT.recordSingleton @Voted22 $ r ^. VF.g20221108VotedAll
    f r = totalReg r F.<+> totalVoted20 r F.<+> totalVoted22 r

regIdMapToFrame :: Map (F.Record BRC.TractLocationR) (F.Record [RegistrationP, TurnoutP, DemIdP]) -> F.FrameRec (BRC.TractLocationR V.++ [RegistrationP, TurnoutP, DemIdP])
regIdMapToFrame = F.toFrame . fmap (uncurry V.rappend) . M.toList

mergeRegTurnoutId :: MC.PSMap BRC.TractLocationR MT.ConfidenceInterval
                     -> MC.PSMap BRC.TractLocationR MT.ConfidenceInterval
                     -> MC.PSMap BRC.TractLocationR MT.ConfidenceInterval
                     -> Either Text (F.FrameRec (BRC.TractLocationR V.++  [RegistrationP, TurnoutP, DemIdP]))
mergeRegTurnoutId regMap turnoutMap idMap = do
  let  missing t = MM.traverseMissing $ \k _ -> Left $ "Missing key=" <> show k <> " in " <> t
       hasRT _ regCI tCI = (regCI, tCI)
       hasRTI _ (r, t) i = r F.&: t F.&: i F.&: V.RNil
  mergeRT <- MM.mergeA (missing "turnoutMap") (missing "regMap") (MM.zipWithMatched hasRT) (MC.unPSMap regMap) (MC.unPSMap turnoutMap)
  regIdMapToFrame <$> MM.mergeA (missing "idMap") (missing "rtMap") (MM.zipWithMatched hasRTI) mergeRT (MC.unPSMap idMap)



exploreTractVoterfile :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r)
                      => BR.CommandLine
                      -> BR.PostInfo
                      -> Text
                      -> K.Sem r ()
exploreTractVoterfile cmdLine pi sa = do
  let geoid r = let x = r ^. GT.tractGeoId in if x < 10000000000 then "0" <> show x else show x

  postPaths <- postPaths "TractTest" cmdLine
  BRK.brNewPost postPaths pi "TractTest" $ do
    vfByTract_C <- VF.voterfileByTracts Nothing
--    states <- K.ignoreCacheTimeM BRL.stateAbbrCrosswalkLoader
    tractsByDistrict <- K.ignoreCacheTimeM BRL.tractsByDistrictLoader

--    let fipsByState = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. GT.stateFIPS)) FL.map) states
--    stateFIPS <- K.knitMaybe ("FIPS lookup failed for " <> sa) $ M.lookup sa fipsByState
    vfByTract <- K.ignoreCacheTime vfByTract_C

    K.logLE K.Info $ "Voter Files By Tract has " <> show (length vfByTract) <> " rows."
    let vfByTractForState = F.filterFrame ((== sa) . view GT.stateAbbreviation) vfByTract
        (regDem, regRep) = FL.fold ((,) <$> FL.premap (view VF.partyDem) FL.sum <*> FL.premap (view VF.partyRep) FL.sum) vfByTractForState
    K.logLE K.Info $ sa <> " voter Files By Tract has " <> show (length vfByTractForState) <> " rows."
    K.logLE K.Info $ sa <> " has " <> show regDem <> " registered Dems and " <> show regRep <> " registered Republicans."

    let regDiff r =
          let ds = realToFrac (r ^. VF.partyDem)
              rs = realToFrac (r ^. VF.partyRep)
          in ds - rs --if ds + rs > 0 then (ds - rs) / (ds + rs) else 0
    for_ [1, 7, 8, 10, 12, 17] $ \cd -> do
      let cdT = show cd
          dCriteria r = r ^. GT.districtTypeC == GT.Congressional && r ^. GT.districtName == cdT
          incTracts = FL.fold (FL.premap geoid FL.set) $ F.filterFrame dCriteria tractsByDistrict
      geoExample postPaths pi ("RegDiff_PA_" <> cdT)
        ("PA " <> cdT <> ": (Reg D - Reg R)/area") (FV.fixedSizeVC 1000 1000 10)
        ("/Users/adam/BlueRipple/bigData/GeoJSON/states/" <> sa <> "_2022_tracts_topo.json","tracts")
        geoid
        (Just incTracts)
        (regDiff, "RegDiff")
        vfByTractForState
        >>= K.addHvega Nothing Nothing
      pure ()


type ModeledRegistrationR = BRC.TractLocationR V.++ '[MR.ModelCI]

modeledRegistrationForState :: (K.KnitEffects r, BRCC.CacheEffects r)
                           => BR.CommandLine
                           -> Text
                           -> K.Sem r (KC.ActionWithCacheTime r (MC.PSMap BRC.TractLocationR MT.ConfidenceInterval))
modeledRegistrationForState cmdLine sa = do
   let cacheStructure psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                               psName "AllCells" sa
       psDataForState :: Text -> DP.PSData  BRC.TractLocationR -> DP.PSData BRC.TractLocationR
       psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

   modeledACSByTractPSData_C <- modeledACSByTract cmdLine BRC.TY2022
   let psD_C = psDataForState sa <$>  modeledACSByTractPSData_C
--   K.ignoreCacheTime psD_C >>= BRLC.logFrame . F.takeRows 100 . DP.unPSData

   let ac = MC.ActionConfig survey (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))
       regModel psName
        = MR.runActionModelAH @BRC.TractLocationR 2022 (cacheStructure psName) MC.Reg ac Nothing
   regModel (sa <> "_Tracts") psD_C

modeledTurnoutForState :: (K.KnitEffects r, BRCC.CacheEffects r)
                           => BR.CommandLine
                           -> Text
                           -> K.Sem r (KC.ActionWithCacheTime r (MC.PSMap BRC.TractLocationR MT.ConfidenceInterval))
modeledTurnoutForState cmdLine sa = do
   let cacheStructure psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                               psName "AllCells" sa
       psDataForState :: Text -> DP.PSData  BRC.TractLocationR -> DP.PSData BRC.TractLocationR
       psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

   modeledACSByTractPSData_C <- modeledACSByTract cmdLine BRC.TY2022
   let psD_C = psDataForState sa <$>  modeledACSByTractPSData_C
--   K.ignoreCacheTime psD_C >>= BRLC.logFrame . F.takeRows 100 . DP.unPSData

   let ac = MC.ActionConfig survey (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))
       turnoutModel psName
        = MR.runActionModelAH @BRC.TractLocationR 2022 (cacheStructure psName) MC.Vote ac Nothing
   turnoutModel (sa <> "_Tracts") psD_C

modeledPartisanIdOfRegForState :: (K.KnitEffects r, BRCC.CacheEffects r)
                           => BR.CommandLine
                           -> Text
                           -> K.Sem r (KC.ActionWithCacheTime r (MC.PSMap BRC.TractLocationR MT.ConfidenceInterval))
modeledPartisanIdOfRegForState cmdLine sa = do
   let cacheStructure psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                               psName "AllCells" sa
       psDataForState :: Text -> DP.PSData  BRC.TractLocationR -> DP.PSData BRC.TractLocationR
       psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

   modeledACSByTractPSData_C <- modeledACSByTract cmdLine BRC.TY2022
   let psD_C = psDataForState sa <$>  modeledACSByTractPSData_C
--   K.ignoreCacheTime psD_C >>= BRLC.logFrame . F.takeRows 100 . DP.unPSData

   let ac = MC.ActionConfig survey (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))
       pc = MC.PrefConfig (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))

       idModel psName
        = MR.runFullModelAH @BRC.TractLocationR 2022 (cacheStructure psName) ac Nothing pc Nothing MR.RegDTargets
   idModel (sa <> "_Tracts") psD_C

geoExample :: (K.KnitEffects r, Foldable f)
           => BR.PostPaths Path.Abs
           -> BR.PostInfo
           -> Text
           -> Text
           -> FV.ViewConfig
           -> (Text, Text)
           -> (row -> Text)
           -> Maybe (Set Text)
           -> (row -> Double, Text)
           -> f row
           -> K.Sem r GV.VegaLite
geoExample pp pi chartID title vc (geoJsonPath, topoJsonFeatureKey) geoid incTractsM (val, valName) rows = do
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
      perValName = valName <> "perSqMi"
--      tComputePer = GV.calculateAs ("2589975 * datum." <> valName <> "/ datum.properties.aland") perValName
      tComputePer = GV.calculateAs ("datum." <> valName <> "/ log(datum.properties.aland)") perValName
      tFilter = maybe id (\gids -> GV.filter $ GV.FOneOf "properties.geoid" $ GV.Strings $ S.toList gids) incTractsM
      transform = (GV.transform . tFilter . tLookup . tComputePer) []
      encValPer = GV.color [GV.MName perValName, GV.MmType GV.Quantitative
                           , GV.MScale [GV.SDomain (GV.DNumbers [-200, 200]), GV.SScheme "redblue" [0,1]]
                           ]
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

tsModelConfig modelId n =  DTM3.ModelConfig True (DTM3.dmr modelId n)
                           DTM3.AlphaHierNonCentered DTM3.ThetaSimple DTM3.NormalDist
{-
modeledACSByTract' :: forall r . (K.KnitEffects r, BRCC.CacheEffects r)
                  => BR.CommandLine -> BRC.TableYear -> K.Sem r (K.ActionWithCacheTime r (DP.PSData BRC.TractLocationR))
modeledACSByTract' cmdLine ty = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  (jointFromMarginalPredictorCSR_ASR_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                              >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                              (Right "CSR_ASR_ByPUMA")
                                              (Right "model/demographic/csr_asr_PUMA")
                                              (DTM3.Model $ tsModelConfig "CSR_ASR_ByPUMA" 71) -- use model not just mean
                                              False -- do not whiten
                                              Nothing Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                               >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                               (Right "CASR_SER_ByPUMA")
                                               (Right "model/demographic/casr_ase_PUMA")
                                               (DTM3.Model $ tsModelConfig "CASR_ASE_ByPUMA" 141) -- use model not just mean
                                               False -- do not whiten
                                               Nothing Nothing Nothing . fmap (fmap F.rcast)
  tracts_C <- BRC.loadACS_2017_2022_Tracts
--  K.logLE K.Info "Sample ACS Tract Rows"
--  K.ignoreCacheTime tracts_C >>= BRLC.logFrame . F.takeRows 100 . BRC.ageSexRace
  let optimalWeightsConfig = DTP.defaultOptimalWeightsAlgoConfig {DTP.owcMaxTimeM = Just 0.1, DTP.owcProbRelTolerance = 1e-4}
  (acsCASERByTract, _products) <- BRC.loadACS_2017_2022_Tracts
                                >>= DMC.predictedCensusCASER' (pure . view GT.stateAbbreviation)
                                DMS.GMDensity
                                (DTP.viaOptimalWeights optimalWeightsConfig DTP.euclideanFull) (Right "model/election2/tractDemographics")
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRCC.retrieveOrMakeD ("model/election2/data/tractPSData" <> BRC.yearsText 2024 ty <> ".bin") acsCASERByTract
    $ \x -> pure . DP.PSData . fmap F.rcast $ (F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)
-}


modeledACSByTract :: forall r . (K.KnitEffects r, BRCC.CacheEffects r)
                   => BR.CommandLine -> BRC.TableYear -> K.Sem r (K.ActionWithCacheTime r (DP.PSData BRC.TractLocationR))
modeledACSByTract cmdLine ty = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  (jointFromMarginalPredictorCSR_ASR_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                              >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                              (Right "CSR_ASR_ByPUMA")
                                              (Right "model/demographic/csr_asr_PUMA")
                                              (DTM3.Model $ tsModelConfig "CSR_ASR_ByPUMA" 71) -- use model not just mean
                                              False -- do not whiten
                                              Nothing Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorASCR_ASE_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                               >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                               (Right "ASCR_ASE_ByPUMA")
                                               (Right "model/demographic/ascr_ase_PUMA")
                                               (DTM3.Model $ tsModelConfig "ASCR_ASE_ByPUMA" 141) -- use model not just mean
                                               False -- do not whiten
                                               Nothing Nothing Nothing . fmap (fmap F.rcast)
  tractTables_C <- BRC.loadACS_2017_2022_Tracts
  let tractsCSR_C = fmap (DMC.recodeCSR @BRC.TractLocationR . fmap F.rcast) $ fmap BRC.citizenshipSexRace tractTables_C
      tractsASR_C =  fmap (fmap (F.rcast @(TractGeoR V.++ DMC.KeysWD DMC.ASR)) . DMC.filterA6FrameToA5 .  DMC.recodeA6SR @BRC.TractLocationR . fmap F.rcast) $ fmap BRC.ageSexRace tractTables_C
      tractsASE_C = fmap (fmap F.rcast) $ fmap BRC.ageSexEducation tractTables_C

  -- CSR x ASR -> CASR
  let casrProdDeps = (,) <$> tractsCSR_C <*> tractsASR_C
  srcaProdOWZ_C <- BRCC.retrieveOrMakeD "analysis/tract-voterfile/casrProdOWZ.bin" casrProdDeps $ \(csr, asr) -> do
    let srcOWZ = FL.fold (DMC.orderedWithZeros @TractGeoR @DMC.SRC @DMC.CSR) csr
        sraOWZ = FL.fold (DMC.orderedWithZeros @TractGeoR @DMC.SRA @DMC.ASR) asr
    K.logTiming (K.logLE K.Info) "CSR x ASR -> OWZs -> SRCA Product" $ DMC.tableProductsOWZ @TractGeoR @DMC.SR @'[DT.CitizenC] @'[DT.Age5C] DMS.GMDensity srcOWZ sraOWZ

  let casrFullDeps = (,) <$> jointFromMarginalPredictorCSR_ASR_C <*> srcaProdOWZ_C
  srcaFullOWZ_C <- BRCC.retrieveOrMakeD "analysis/tract-voterfile/srcaFullOWZ.bin" casrFullDeps $ \(predCSR_ASR, srcaProd) -> do
    let cMatrix = DMC.logitMarginalsCMatrix @DMC.SR @'[DT.CitizenC] @'[DT.Age5C]
        covAndProdS = DMC.covariatesAndProdVs (DMC.tp3CovariatesAndProdVs @TractGeoR @DMC.SRCA cMatrix) srcaProd
        ms = DMC.marginalStructure @DMC.SRCA @'[DT.CitizenC] @'[DT.Age5C] @DMS.CellWithDensity @DMC.SR DMS.cwdWgtLens DMS.innerProductCWD
    nv <- K.knitMaybe "nullVecsSRCA: Bad marginal subsets?" $ DTP.nullVecsMS ms Nothing
    K.logTiming (K.logLE K.Info) "SRCA Product -> SRCA Full (predict & on-simplex)"
      $ DMC.tablePredictions @_ @DMC.SRCA (DMC.tp3Predict (view GT.stateAbbreviation) predCSR_ASR) DMC.cvpProd DMC.cvpGeoKey (DMC.nnlsOnSimplexer nv) covAndProdS

  let ascreProdDeps = (,) <$> srcaFullOWZ_C <*> tractsASE_C
  ascreProdOWZ_C <- BRCC.retrieveOrMakeD "analysis/tract-voterfile/ascreProdOWZ.bin" ascreProdDeps $ \(srcaOWZ, ase) -> do
    let aseOWZ = FL.fold (DMC.orderedWithZeros @TractGeoR @DMC.ASE @DMC.ASE) ase
    K.logTiming (K.logLE K.Info) "SRCA x ASE -> OWZ -> ASRCE Product"
      $ DMC.tableProductsOWZ @TractGeoR @DMC.AS @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.SRCA @DMC.ASE DMS.GMDensity srcaOWZ aseOWZ

  let ascreFullDeps = (,) <$> jointFromMarginalPredictorASCR_ASE_C <*> ascreProdOWZ_C
  ascreFullOWZ_C <- BRCC.retrieveOrMakeD "analysis/tract-voterfile/ascreFullOWZ.bin" ascreFullDeps $ \(predSRCA_ASE, ascreProd) -> do
    let cMatrix = DMC.logitMarginalsCMatrix @DMC.AS @'[DT.CitizenC, DT.Race5C] @'[DT.Education4C]
        covAndProdS = DMC.covariatesAndProdVs (DMC.tp3CovariatesAndProdVs @TractGeoR @DMC.ASCRE cMatrix) ascreProd
        ms = DMC.marginalStructure @DMC.ASCRE @'[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMS.CellWithDensity @DMC.AS DMS.cwdWgtLens DMS.innerProductCWD
    nv <- K.knitMaybe "nullVecsASCRE: Bad marginal subsets?" $ DTP.nullVecsMS ms Nothing
    K.logTiming (K.logLE K.Info) "ASCRE Product -> ASCRE Full (predict & on-simplex)"
      $ DMC.tablePredictions @_ @DMC.ASCRE (DMC.tp3Predict (view GT.stateAbbreviation) predSRCA_ASE) DMC.cvpProd DMC.cvpGeoKey (DMC.nnlsOnSimplexer nv) covAndProdS

  BRCC.retrieveOrMakeD "analysis/tract-voterfile/aserPSData.bin" ascreFullOWZ_C $ \ascreFullOWZ -> do
    let DMC.OrderedWithZeros fr _ = ascreFullOWZ
    K.logTiming (K.logLE K.Info) "ASCRE Full -> ASER PSData"
      $ pure $ DP.PSData $ fmap F.rcast $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) fr

--  pure ()

type ASRR = '[BR.Year, GT.StateAbbreviation] V.++ BRC.TractLocationR V.++ [DT.Age6C, DT.SexC, BRC.RaceEthnicityC, DT.PopCount]

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


compareCVAPs :: (K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> Text -> K.Sem r ()
compareCVAPs cmdLine sa = do
  let filterByState :: (FC.ElemsOf rs '[GT.StateAbbreviation], FSI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
      filterByState = F.filterFrame ((== sa) . view GT.stateAbbreviation)
      lt = K.logTiming (K.logLE K.Info)
  modeledACSByTractPSData_C <- modeledACSByTract cmdLine BRC.TY2022
  cvapFromFull_C <- fmap filterByState
                    <$> BRCC.retrieveOrMakeFrame "analysis/tract-voterfile/tractCVAPs.bin" modeledACSByTractPSData_C (lt "tractCVAPs" . pure . tractCVAPs)
  fullC <- K.ignoreCacheTime cvapFromFull_C
  tractTables_C <- BRC.loadACS_2017_2022_Tracts
  tractTables <- K.ignoreCacheTime tractTables_C
  let (recodedC, recodedAll) = tractCVAPFromRecodedCSR tractTables
      (csrC, csrAll) = tractCVAPFromCSR tractTables
      aseAll = tractCVAPFromASE tractTables
      cL = zip3 (toList fullC) (toList (filterByState recodedC)) $ toList (filterByState csrC)
      allL = zip3 (toList (filterByState recodedAll)) (toList (filterByState csrAll)) $ toList (filterByState aseAll)
  putTextLn $ show cL
  putTextLn $ show allL




tractCVAPs :: DP.PSData BRC.TractLocationR -> F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount]
tractCVAPs = FL.fold fld . DP.unPSData where
  fld = FMR.concatFold
        $ FMR.mapReduceFold
        FMR.noUnpack
        (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
        (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)


tractCVAPFromCSR :: BRC.LoadedCensusTablesByTract
                 -> (F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount]
                    , F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount])
tractCVAPFromCSR ct = FL.fold ((,) <$> fldC <*> fldAll) $ BRC.citizenshipSexRace ct
  where
    fldC = FMR.concatFold
           $ FMR.mapReduceFold
           (FMR.unpackFilterOnField @BRC.CitizenshipC (/= BRC.NonCitizen))
           (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
           (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
    fldAll = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
             (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)


tractCVAPFromASE :: BRC.LoadedCensusTablesByTract
                 -> (F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount])
tractCVAPFromASE ct = FL.fold fldAll $ BRC.ageSexEducation ct
  where
    fldAll = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
             (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)


tractCVAPFromRecodedCSR :: BRC.LoadedCensusTablesByTract
                        -> (F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount]
                           ,F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount]
                           )
tractCVAPFromRecodedCSR ct = FL.fold ((,) <$> fldC <*> fldAll) $ DMC.recodeCSR @[GT.StateAbbreviation, GT.TractGeoId] $ BRC.citizenshipSexRace ct
  where
    fldC = FMR.concatFold
           $ FMR.mapReduceFold
           (FMR.unpackFilterOnField @DT.CitizenC (== DT.Citizen))
           (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
           (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
    fldAll = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
             (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)

tractCVAPFromCSR_ASR :: (K.KnitEffects r, BRCC.CacheEffects r)
                     => BRC.LoadedCensusTablesByTract -> K.Sem r (F.FrameRec [GT.StateAbbreviation, GT.TractGeoId, DT.PopCount])
tractCVAPFromCSR_ASR ct = do
  let
    csr =  DMC.recodeCSR @BRC.TractLocationR  $ BRC.citizenshipSexRace ct
    asr = fmap (F.rcast @(TractGeoR V.++ DMC.KeysWD DMC.ASR)) . DMC.filterA6FrameToA5 .  DMC.recodeA6SR @BRC.TractLocationR $ BRC.ageSexRace ct
    srcOWZ = FL.fold (DMC.orderedWithZeros @TractGeoR @DMC.SRC @DMC.CSR) csr
    sraOWZ = FL.fold (DMC.orderedWithZeros @TractGeoR @DMC.SRA @DMC.ASR) asr
  (DMC.OrderedWithZeros srca _) <-  DMC.tableProductsOWZ @TractGeoR @DMC.SR @'[DT.CitizenC] @'[DT.Age5C] DMS.GMDensity srcOWZ sraOWZ
  let fld = FMR.concatFold
        $ FMR.mapReduceFold
        (FMR.unpackFilterOnField @DT.CitizenC (== DT.Citizen))
        (FMR.assignKeysAndData @[GT.StateAbbreviation, GT.TractGeoId] @'[DT.PopCount])
        (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  pure $ FL.fold fld srca
