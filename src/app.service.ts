import { Injectable } from "@nestjs/common";
import { FeedId, FeedValueData } from "./dto/provider-requests.dto";
import { BaseDataFeed } from "./data-feeds/base-feed";

@Injectable()
export class ExampleProviderService {
  constructor(private readonly dataFeed: BaseDataFeed) {}

  async getValue(feed: FeedId, votingRoundId: number): Promise<FeedValueData> {
    return this.dataFeed.getValue(feed, votingRoundId);
  }

  async getValues(feeds: FeedId[], votingRoundId: number): Promise<FeedValueData[]> {
    return this.dataFeed.getValues(feeds, votingRoundId);
  }
}
