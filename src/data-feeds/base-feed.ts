import { FeedId, FeedValueData } from "../dto/provider-requests.dto";

export abstract class BaseDataFeed {
  abstract getValue(feed: FeedId, votingRoundId: number): Promise<FeedValueData>;
  abstract getValues(feeds: FeedId[], votingRoundId: number): Promise<FeedValueData[]>;
}
