/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.source.twitter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.MediaEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.UserStream;
import twitter4j.UserList;
import twitter4j.DirectMessage;
import twitter4j.UserStreamListener;
// Flume twitter4j library has DataObjectFactory, not TwitterObjectFactory
import twitter4j.json.DataObjectFactory;


/**
 * A demo Flume source that connects to the UserStream Streaming API.
 * Unlike TwitterSource, it downloads raw data in JSON and sends to a downstream
 * Flume sink.
 *
 * Requires the consumer and access tokens and secrets of a Twitter developer account
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TwitterUserStreamToJSONSource
    extends AbstractSource
    implements EventDrivenSource, Configurable, UserStreamListener {

  private TwitterStream twitterStream;

  private long docCount = 0;
  private long startTime = 0;
  private long exceptionCount = 0;
  private long totalTextIndexed = 0;
  private long skippedDocs = 0;
  private long batchEndTime = 0;
  private final List<String> docs = new ArrayList<String>();
  private final ByteArrayOutputStream serializationBuffer =
      new ByteArrayOutputStream();
  private final Charset charset = Charset.forName("UTF-8");

  private int maxBatchSize = 1000;
  private int maxBatchDurationMillis = 1000;

  // Fri May 14 02:52:55 +0000 2010
  private SimpleDateFormat formatterTo =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
  private DecimalFormat numFormatter = new DecimalFormat("###,###.###");

  private static int REPORT_INTERVAL = 100;
  private static int STATS_INTERVAL = REPORT_INTERVAL * 10;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TwitterUserStreamToJSONSource.class);

  public TwitterUserStreamToJSONSource() {
  }

  @Override
  public void configure(Context context) {
    String consumerKey       = context.getString("consumerKey");
    String consumerSecret    = context.getString("consumerSecret");
    String accessToken       = context.getString("accessToken");
    String accessTokenSecret = context.getString("accessTokenSecret");
    String proxyHost     = context.getString("http.proxyHost");
    String proxyPort     = context.getString("http.proxyPort");
    String proxyUser     = context.getString("http.proxyUser");
    String proxyPassword = context.getString("http.proxyPassword");
    ConfigurationBuilder cb  = new ConfigurationBuilder();
    cb.setDebugEnabled(true);
    if (StringUtils.isNotEmpty(proxyHost))
      cb.setHttpProxyHost(proxyHost);
    if (StringUtils.isNotEmpty(proxyPort))
      cb.setHttpProxyPort(Integer.valueOf(proxyPort));
    if (StringUtils.isNotEmpty(proxyUser))
      cb.setHttpProxyUser(proxyUser);
    if (StringUtils.isNotEmpty(proxyPassword))
      cb.setHttpProxyPassword(proxyPassword);
    cb.setJSONStoreEnabled(true);

    LOGGER.info("Consumer Key:        '" + consumerKey + "'");
    LOGGER.info("Consumer Secret:     '" + consumerSecret + "'");
    LOGGER.info("Access Token:        '" + accessToken + "'");
    LOGGER.info("Access Token Secret: '" + accessTokenSecret + "'");

    twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
    twitterStream.setOAuthAccessToken(new AccessToken(accessToken,
                                                      accessTokenSecret));
    twitterStream.addListener(this);

    maxBatchSize = context.getInteger("maxBatchSize", maxBatchSize);
    maxBatchDurationMillis = context.getInteger("maxBatchDurationMillis",
                                                maxBatchDurationMillis);
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting twitter source {} ...", this);
    docCount = 0;
    startTime = System.currentTimeMillis();
    exceptionCount = 0;
    totalTextIndexed = 0;
    skippedDocs = 0;
    batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

    // twitterStream.sample();
    twitterStream.user();

    LOGGER.info("Twitter source {} started.", getName());
    // This should happen at the end of the start method, since this will 
    // change the lifecycle status of the component to tell the Flume 
    // framework that this component has started. Doing this any earlier 
    // tells the framework that the component started successfully, even 
    // if the method actually fails later.
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Twitter source {} stopping...", getName());
    twitterStream.shutdown();
    super.stop();
    LOGGER.info("Twitter source {} stopped.", getName());
  }

  public void onStatus(Status status)  {
    // create a new json with event type and data
    String doc = "{" +
      "\"eventType\":\"onStatus\", " +
      "\"status\":" + DataObjectFactory.getRawJSON(status) + "}";
    docs.add(doc);
    
    if (docs.size() >= maxBatchSize ||
        System.currentTimeMillis() >= batchEndTime) {
      batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

      List<Event> events;
      try {
        events = buildEvents(docs);
      } catch (IOException e) {
        LOGGER.error("Exception while serializing tweet", e);
        return; //skip
      }
      // getChannelProcessor().processEvent(event); // send event to the flume sink
      getChannelProcessor().processEventBatch(events); // send event to the flume sink
      docs.clear();
    }
    
    docCount++;
    if ((docCount % REPORT_INTERVAL) == 0) {
      LOGGER.info(String.format("Processed %s docs",
                                numFormatter.format(docCount)));
    }
    if ((docCount % STATS_INTERVAL) == 0) {
      logStats();
    }
  }

  private List<Event> buildEvents(List<String> docList)
      throws IOException {
    List<Event> events = new ArrayList<Event>();

    for(String doc : docList) {
      serializationBuffer.reset();
      serializationBuffer.write(doc.getBytes(charset));

      Event event = EventBuilder.withBody(serializationBuffer.toByteArray());
      events.add(event);
    };

    return events;
  }

  private void logStats() {
    double mbIndexed = totalTextIndexed / (1024 * 1024.0);
    long seconds = (System.currentTimeMillis() - startTime) / 1000;
    seconds = Math.max(seconds, 1);
    LOGGER.info(String.format("Total docs indexed: %s, total skipped docs: %s",
                numFormatter.format(docCount), numFormatter.format(skippedDocs)));
    LOGGER.info(String.format("    %s docs/second",
                numFormatter.format(docCount / seconds)));
    LOGGER.info(String.format("Run took %s seconds and processed:",
                numFormatter.format(seconds)));
    LOGGER.info(String.format("    %s MB/sec sent to index",
                numFormatter.format(((float) totalTextIndexed / (1024 * 1024)) / seconds)));
    LOGGER.info(String.format("    %s MB text sent to index",
                numFormatter.format(mbIndexed)));
    LOGGER.info(String.format("There were %s exceptions ignored: ",
                numFormatter.format(exceptionCount)));
  }

  // StatusListener abstract methods.
  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    // Do nothing...
  }
  public void onScrubGeo(long userId, long upToStatusId) {
    // Do nothing...
  }
  public void onStallWarning(StallWarning warning) {
    // Do nothing...
  }
  public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
    // Do nothing...
  }

  // UserStreamListener abstract methods.
  public void onDeletionNotice(long directMessageId, long userId) {
  }
  public void onFriendList(long[] friendIds) {
  }
  public void onFavorite(User source, User target, Status favoritedStatus) {
  }
  public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
  }
  public void onFollow(User source, User followedUser) {
  }
  public void onUnfollow(User source, User unfollowedUser) {
  }
  public void onDirectMessage(DirectMessage directMessage) {
  }
  public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {
  }
  public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {
  }
  public void onUserListSubscription(User subscriber, User listOwner, UserList list) {
  }
  public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {
  }
  public void onUserListCreation(User listOwner, UserList list) {
  }
  public void onUserListUpdate(User listOwner, UserList list) {
  }
  public void onUserListDeletion(User listOwner, UserList list) {
  }
  public void onUserProfileUpdate(User updatedUser) {
  }
  public void onUserSuspension(long suspendedUser) {
  }
  public void onUserDeletion(long deletedUser) {
  }
  public void onBlock(User source, User blockedUser) {
  }
  public void onUnblock(User source, User unblockedUser) {
  }
  public void onRetweetedRetweet(User source,User target, Status retweetedStatus) {
  }
  public void onFavoritedRetweet(User source,User target, Status favoritedRetweeet) {
  }
  public void onQuotedTweet(User source, User target, Status quotingTweet) {
  }

  public void onException(Exception e) {
    LOGGER.error("Exception while streaming tweets", e);
  }
}



