require 'sensu-plugin/cli'
require 'json'

module Sensu
  module Plugin
    class Metric
      class CLI < Sensu::Plugin::CLI
        def to_json(obj = nil)
          if obj.is_a?(String) || obj.is_a?(Exception)
            puts obj.to_s
          elsif obj.is_a?(Hash)
            obj['timestamp'] ||= Time.now.to_i
            puts ::JSON.generate(obj)
          end
        end

        # Outputs metrics using the Statsd datagram format
        #
        # @param args [Array<String, Int>] list of arguments
        # @note the argument order should be:
        #   `metric_path`: Mandatory, name for the metric,
        #   `value`: Mandatory, metric value
        #   `timestamp`: Optional, unix timestamp, defaults to current time
        # @return [String] formated metric data
        def to_graphite(*args)
          return if args.empty?
          if args[0].is_a?(Exception) || args[1].nil?
            puts args[0].to_s
          else
            args[2] ||= Time.now.to_i
            puts args[0..2].join("\s")
          end
        end

        # Outputs metrics using the Statsd datagram format
        #
        # @param args [Array<String, Int>] list of arguments
        # @note the argument order should be:
        #   `metric_name`: Mandatory, name for the metric,
        #   `value`: Mandatory, metric value
        #   `type`: Optional, metric type- `c` for counter, `g` for gauge, `ms` for timer, `s` for set
        # @return [String] formated metric data
        def to_statsd(*args)
          return if args.empty?
          if args[0].is_a?(Exception) || args[1].nil?
            puts args[0].to_s
          else
            type = args[2] || 'kv'
            puts [args[0..1].join(':'), type].join('|')
          end
        end

        # Outputs metrics using the DogStatsd datagram format
        #
        # @param args [Array<String, Int>] list of arguments
        # @note the argument order should be:
        #   `metric_name`: Mandatory, name for the metric,
        #   `value`: Mandatory, metric value
        #   `type`: Optional, metric type- `c` for counter, `g` for gauge, `ms` for timer, `h` for histogram, `s` for set
        #   `tags`: Optional, a comma separated key:value string `tag1:value1,tag2:value2`
        # @return [String] formated metric data
        def to_dogstatsd(*args)
          return if args.empty?
          if args[0].is_a?(Exception) || args[1].nil?
            puts args[0].to_s
          else
            type = args[2] || 'kv'
            tags = args[3] ? "##{args[3]}" : nil
            puts [args[0..1].join(':'), type, tags].compact.join('|')
          end
        end

        # Outputs metrics using the InfluxDB line protocol format
        #
        # @param args [Array<String, Int>] list of arguments
        # @note the argument order should be:
        #   `measurement_name`: Mandatory, name for the InfluxDB measurement,
        #   `fields`: Mandatory, either an integer or a comma separated key=value string `field1=value1,field2=value2`
        #   `tags`: Optional, a comma separated key=value string `tag1=value1,tag2=value2`
        #   `timestamp`: Optional, unix timestamp, defaults to current time
        # @return [String] formated metric data
        def to_influxdb(*args)
          return if args.empty?
          if args[0].is_a?(Exception) || args[1].nil?
            puts args[0].to_s
          else
            fields = if args[1].is_a?(Integer)
                       "value=#{args[1]}"
                     else
                       args[1]
                     end
            measurement = [args[0], args[2]].compact.join(',')
            ts = args[3] || Time.now.to_i
            puts [measurement, fields, ts].join(' ')
          end
        end

        # metric_name
        # value=nil
        # tags={}
        # timestamp=Time.now.to_i
        # graphite_metric_path="#{metric}"
        # statsd_metric_name="#{metric}"
        # statsd_type=nil
        # influxdb_measurement="#{metric}"
        # influxdb_fields="#{value}"
        # json_obj
        def to_generic(type='graphite', metric = {})
          tags = metric[:tags] || []

          case type
          when 'json'
            json_obj = metric[:json_obj] || {
              metric_name: metric[:metric_name],
              value: metric[:value],
              timestamp: metric[:timestamp],
              tags: tags
            }
            to_json json_obj
          when 'graphite'
            graphite_metric_path = metric[:graphite_metric_path] ||
                                   metric[:metric_name]
            to_graphite graphite_metric_path, metric[:value], metric[:timestamp]
          when 'statsd'
            statsd_metric_name = metric[:statsd_metric_name] ||
                                 metric[:metric_name]
            to_statsd statsd_metric_name, metric[:value], metric[:statsd_type]
          when 'dogstatsd'
            dogstatsd_metric_name = metric[:dogstatsd_metric_name] ||
                                    metric[:statsd_metric_name] ||
                                    metric[:metric_name]
            dogstatsd_type = metric[:dogstatsd_type] || metric[:statsd_type]
            dogstatsd_tags = tags.map { |k, v| "#{k}:#{v}" }.join(',')
            to_dogstatsd dogstatsd_metric_name, metric[:value],
                         dogstatsd_type, dogstatsd_tags
          when 'influxdb'
            influxdb_measurement = metric[:influxdb_measurement] ||
                                   metric[:metric_name]
            influxdb_fields = metric[:influxdb_fields] ||
                              metric[:value]
            influxdb_tags = tags.map { |k, v| "#{k}=#{v}" }.join(',')
            to_influxdb influxdb_measurement, influxdb_fields,
                        influxdb_tags, metric[:timestamp]
          end
        end


        class JSON < Sensu::Plugin::Metric::CLI
          def output(*args)
            to_json(*args)
          end
        end

        class Graphite < Sensu::Plugin::Metric::CLI
          def output(*args)
            to_graphite(*args)
          end
        end

        class Statsd < Sensu::Plugin::Metric::CLI
          def output(*args)
            to_statsd(*args)
          end
        end

        class Dogstatsd < Sensu::Plugin::Metric::CLI
          def output(*args)
            to_dogstatsd(*args)
          end
        end

        class Influxdb < Sensu::Plugin::Metric::CLI
          def output(*args)
            to_influxdb(*args)
          end
        end

        class Generic < Sensu::Plugin::Metric::CLI
          option :metric_format,
                 short: '-f METRIC_FORMAT',
                 long: '--metric_format METRIC_FORMAT',
                 in: ['json', 'graphite', 'statsd', 'dogstatsd', 'influxdb'],
                 default: 'graphite'

          def output(*args)
            to_generic(type=config[:metric_format], *args)
          end

        end
      end
    end
  end
end
