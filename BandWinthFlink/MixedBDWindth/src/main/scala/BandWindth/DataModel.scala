package BandWindth

case class ESLogFormat(
                        AppId: String
                        , EventType: String
                        , QoSType: String
                        , SdkVersion: String
                        , ReportInterval: Long
                        //@JSONField(name="timestamp")
                        , Timestamp: String
                        , Url: String
                        , PeerId: String
                        , AllDownload: Long
                        , HcdnDownload: Long
                        , TrdCdnDownload: Long
                        , AllSpeed: Long
                        , HcdnSpeed: Int
                        , TrdCdnSpeed: Long
                        , HcdnWaste: Long
                        , TrdCdnWaste: Long
                        , InternalWaste: Long
                        , ShareRate: Double
                        , StartTime: String
                        , StreamFramerate: Double
                        , FrameRate: Int
                        , StuckCount: Int
                        , StuckDuration: Int
                        , PeerList: String
                        , PlayStartDelay: Long
                        , PlayDuration: Long
                        , SwitchFlowInterval: Int
                        , IsSeamlessSwitch: String
                        , SwitchTime: Int
                        , PeerType: String
                        , ConnectTime: Int
                        , Status: Int
                        , Reason: String
                        , ConnectionDuration: Long
                      )

//***************** 输出表 *********************

/**
 * uv独立访客
 * @param Appid
 * @param Url
 * @param timestamp
 * @param count
 */
case class Uvcount(
                    Appid: String
                    , Url: String
                    , timestamp: Long
                    , dataTime: String
                    , count: Int
                  )
/**
 * 延波时长 延波次数
 * @param Appid
 * @param Url
 * @param timestamp
 * @param startDelay
 * @param count
 */
case class DelayTime(
                      Appid: String
                      , Url: String
                      , timestamp: Long
                      , datatime: String     //临时
                      , startDelay: Long
                      , count: Int
                    )

/**
 * 带宽
 * @param AppId
 * @param Url
 * @param timestamp
 * @param AllDownload
 * @param HCdnDownload
 * @param TrdCdnDownload
 * @param HcdnWaste
 * @param InternalWaste
 * @param x
 * @param AllBW
 * @param HCdnBW
 * @param CdnBW
 */
case class BandwidthSum(
                         AppId: String
                         , Url: String
                         , timestamp: Long
                         , datatime: String     //临时
                         , AllDownload: Long
                         , HCdnDownload: Long
                         , TrdCdnDownload: Long
                         , HcdnWaste: Long
                         , InternalWaste: Long
                         , x: Long
                         , AllBW: Double // long
                         , HCdnBW: Double
                         , CdnBW: Double
                       )

/**
 * 总的
 * @param bdsum
 * @param uv
 * @param delay
 */
case class Stat(
                 bdsum:BandwidthSum,
                 uv:Uvcount,
                 delay:DelayTime
               )