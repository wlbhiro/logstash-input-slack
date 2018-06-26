# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

class LogStash::Inputs::Slack < LogStash::Inputs::Base
  config_name "slack"
  milestone 1

  # Slack token
  config :token, :validate => :string, :required => true

  # interval token
  config :interval, :validate => :number, :default => (60 * 15)
  
  public
  def register
    require 'rest-client'
    require 'cgi'
    require 'json'

    @content_type = "application/x-www-form-urlencoded"
  end # def register

  public
  def run(queue)
    get_channels_url = "https://slack.com/api/channels.list?token="+@token
    get_message_url = "https://slack.com/api/channels.history?count=1000&token="+@token+"&channel="
    get_users_url = "https://slack.com/api/users.list?token="+@token
    p get_channels_url
    Stud.interval(@interval) do
      @logger.info('Polling Slack API')

      @users = {}
      # GET USER LIST
      begin
        # p get_users_url
        RestClient.get(
          get_users_url,
          :accept => "application/json",
          :'User-Agent' => "logstash-input-slack"
          ) { |response, request, result, &block|
            if response.code != 200
              @logger.warn("Got a #{response.code} response: #{response}")
            end
            JSON.parse(response)['members'].each do |member|
              @users[member['id']] = member['real_name']
            end
            # p @users
          }
      rescue Exception => e
        @logger.warn("Unhandled exception", :exception => e,
                    :stacktrace => e.backtrace)
      end

      @channels = {}
      # GET CHANNEL LIST
      begin
        # p get_channels_url
        RestClient.get(
          get_channels_url,
          :accept => "application/json",
          :'User-Agent' => "logstash-input-slack"
          ) { |response, request, result, &block|
            if response.code != 200
              @logger.warn("Got a #{response.code} response: #{response}")
            end
            JSON.parse(response)['channels'].each do |channel|
              @channels[channel['id']] = channel['name']
            end
            # p @channels
          }
      rescue Exception => e
        @logger.warn("Unhandled exception", :exception => e,
                    :stacktrace => e.backtrace)
      end

      # GET MESSAGE CHANNELS
      @channels.keys.each do |channel|
        begin
          # p get_message_url+CGI.escape(channel)
          RestClient.get(
            get_message_url+CGI.escape(channel),
            :accept => "application/json",
            :'User-Agent' => "logstash-input-slack"
            ) { |response, request, result, &block|
              if response.code != 200
                @logger.warn("Got a #{response.code} response: #{response}")
              end
              # p response
              JSON.parse(response)['messages'].each do |message|
                # p message
                event = LogStash::Event.new(message)
                event.set("host", "slack-"+@channels[channel]+"-"+slack_replace(@users, @channels, message['user']))
                event.set("channel", @channels[channel])
                event.set("message", slack_replace(@users, @channels, message['text']))
                event.set("message_raw", message['text'])
                event.set("slack_timestamp", message['ts'])
                queue << event
              end
            }
        rescue Exception => e
          @logger.warn("Unhandled exception", :exception => e,
                      :stacktrace => e.backtrace)
        end
      end
    end # loop
  end # def run

  private
  def slack_replace(users, channels, text) 
    return hash_replace(channels, hash_replace(users, text))
  end

  private
  def hash_replace(hash, text) 
    hash.keys.each do |key|
      # p key
      value = hash[key]
      if value == nil
        value = ""
      end
      text = text.gsub(key, value)
    end
    return text
  end
end # class LogStash::Inputs::Slack
