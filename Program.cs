using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;
public class MitchReader{
    const byte StartOfMessageIndicator = 255;
    /// <summary>
    /// StartCapture: Reads the MITCH extraction data provided as a FileInfo object /// </summary>
    /// <param name="deviceFile"></param>
    public static void StartCaptureSample(FileInfo deviceFile, string jsonFileName){
        var messageCount = 0;
        using (var fileStream = new FileStream(deviceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var binaryReader = new BinaryReader(fileStream)){
            using(StreamWriter writer = new StreamWriter(jsonFileName)){

                writer.WriteLine("{ \"data\" : [");
                do{
                    messageCount++;

                    if (messageCount%100000 == 0){
                        Console.WriteLine("{0} Messages Processed", messageCount);
                    }
                    try{
                        var messageWithHeader = GetMessageWithHeader(binaryReader);
                        if (messageWithHeader.HasValue){
                            var messageDataBuilder = new StringBuilder();
                            Array.ForEach(messageWithHeader.Value.Data, x => messageDataBuilder.Append(x+","));

                            using (var memoryStream = new MemoryStream(messageWithHeader.Value.Data)){
                                using (var dataBinaryReader = new BinaryReader(memoryStream)){

                                    switch(messageWithHeader.Value.MessageType){
                                        case "53":
                                            var systemEventMessageBody = SystemEvent.BuildSystemEventMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var systemEventMessageBodyJSON = JsonConvert.SerializeObject(systemEventMessageBody);

                                            // Console.WriteLine(systemEventMessageBodyJSON);
                                            // Console.WriteLine(systemEventMessageBody.MessageTypeString);

                                            writer.WriteLine(systemEventMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "54":
                                            var timeMessageBody = TimeMessage.BuildTimeMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var timeMessageBodyJSON = JsonConvert.SerializeObject(timeMessageBody);

                                            // Console.WriteLine(timeMessageBodyJSON);
                                            // Console.WriteLine(timeMessageBody.MessageTypeString);

                                            writer.WriteLine(timeMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "41":
                                            var addOrderMessageBody = AddOrderMessage.BuildAddOrderMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var addOrderMessageBodyJSON = JsonConvert.SerializeObject(addOrderMessageBody);
                                            //Console.WriteLine(addOrderMessageBodyJSON);
                                            writer.WriteLine(addOrderMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "46":
                                            var addAttributedOrderMessageBody = AddAttributedOrderMessage.BuildAddAttributedOrderMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var addAttributedOrderMessageBodyJSON = JsonConvert.SerializeObject(addAttributedOrderMessageBody);
                                            //Console.WriteLine(addAttributedOrderMessageBodyJSON);
                                            writer.WriteLine(addAttributedOrderMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "52":
                                            var symbolDirectoryMessageBody = SymbolDirectoryMessage.BuildSymbolDirectoryMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var symbolDirectoryMessageBodyJSON = JsonConvert.SerializeObject(symbolDirectoryMessageBody);
                                            //Console.WriteLine(symbolDirectoryMessageBodyJSON);
                                            writer.WriteLine(symbolDirectoryMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "44":
                                            var orderDeletedMessageBody = OrderDeletedMessage.BuildOrderDeletedMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var orderDeletedMessageBodyJSON = JsonConvert.SerializeObject(orderDeletedMessageBody);
                                            //Console.WriteLine(orderDeletedMessageBodyJSON);
                                            writer.WriteLine(orderDeletedMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "55":
                                            var orderModifiedMessageBody = OrderModifiedMessage.BuildOrderModifiedMessage(dataBinaryReader,messageWithHeader.Value.Id);
                                            var orderModifiedMessageBodyJSON = JsonConvert.SerializeObject(orderModifiedMessageBody);
                                            //Console.WriteLine(orderModifiedMessageBodyJSON);
                                            writer.WriteLine(orderModifiedMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "79":
                                            var orderBookClearMessageBody = OrderBookClearMessage.BuildOrderBookClearMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var orderBookClearMessageBodyJSON =  JsonConvert.SerializeObject(orderBookClearMessageBody);
                                            //Console.WriteLine(orderBookClearMessageBodyJSON);
                                            writer.WriteLine(orderBookClearMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "45":
                                            var orderExecutedMessageBody = OrderExecutedMessage.BuildOrderExecutedMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var orderExecutedMessageBodyJSON = JsonConvert.SerializeObject(orderExecutedMessageBody);
                                            //Console.WriteLine(orderExecutedMessageBodyJSON);
                                            writer.WriteLine(orderExecutedMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "43":
                                            var orderExecutedWithPriceSizeMessageBody = OrderExecutedWithPriceSizeMessage.BuildOrderExecutedWithPriceSizeMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var orderExecutedWithPriceSizeMessageBodyJSON = JsonConvert.SerializeObject(orderExecutedWithPriceSizeMessageBody);
                                            //Console.WriteLine(orderExecutedWithPriceSizeMessageBodyJSON);
                                            writer.WriteLine(orderExecutedWithPriceSizeMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "50":
                                            var tradeMessageBody = TradeMessage.BuildTradeMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var tradeMessageBodyJSON = JsonConvert.SerializeObject(tradeMessageBody);
                                            //Console.WriteLine(tradeMessageBodyJSON);
                                            writer.WriteLine(tradeMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "51":
                                            var tradeAuctionMessageBody = AuctionTradeMessage.BuildAuctionTradeMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var tradeAuctionMessageBodyJSON = JsonConvert.SerializeObject(tradeAuctionMessageBody);
                                            //Console.WriteLine(tradeAuctionMessageBodyJSON);
                                            writer.WriteLine(tradeAuctionMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        case "78":
                                            var offBookTradeMessageBody = OffBookTradeMessage.BuildOffBookTradeMessage(dataBinaryReader, messageWithHeader.Value.Id);
                                            var offBookTradeMessageBodyJSON = JsonConvert.SerializeObject(offBookTradeMessageBody);
                                            // Console.WriteLine(offBookTradeMessageBodyJSON);
                                            writer.WriteLine(offBookTradeMessageBodyJSON);
                                            writer.WriteLine(",");
                                            break;
                                        default:
                                            var messageWithHeaderJSON = JsonConvert.SerializeObject(messageWithHeader);
                                            writer.WriteLine(messageWithHeaderJSON);
                                            writer.WriteLine(",");
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    catch (EndOfStreamException){
                        break;
                    }
                } while (true);
                writer.WriteLine("{}");
                writer.WriteLine("]}");
            }
        }
    }
    /// <summary>
    /// Returns the Constructed Message or null if the start of message inidicator is not read.
    /// </summary>
    /// <param name="binaryReader"></param>
    /// <returns></returns>
    public static ExtractionMessage? GetMessageWithHeader(BinaryReader binaryReader){
        if (binaryReader.ReadByte() == StartOfMessageIndicator){
            return ExtractionMessage.BuildExtractionMessage(binaryReader);
        }
        return null;
    }
    // public static byte[] CorrectEndianness(this byte[] value, Endianness endianness)
    // {
    //     switch (endianness)
    //     {
    //         case Endianness.LittleEndian:
    //             if (!BitConverter.IsLittleEndian) Array.Reverse(value);
    //             break;
    //         case Endianness.BigEndian:
    //             if (BitConverter.IsLittleEndian) Array.Reverse(value);
    //             break;
    //         case Endianness.Default:
    //             break;
    //     }
    //     return value;
    // }
    
    public struct ExtractionMessage{
        public ushort Length;
        public byte Crc;
        public long Id;
        public string MessageType;
        public byte[] Data;
        public string MessageTypeString;
        /// <summary>
        /// Constructs a ExtractionMessage structure from a list of bytes
        /// </summary>
        /// <param name="binaryReader"></param>
        /// <returns></returns>
        public static ExtractionMessage BuildExtractionMessage(BinaryReader binaryReader){
            const int bytesInLong = 8;
            const int bytesInShort = 2;
            const int bytesInMessageType = 4;
            const int bytesInUnitHeaderLength = 4;

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var shortBuffer = BitConverter.ToUInt16(lengthBuffer, 0);
            var payloadLength = shortBuffer - bytesInLong - bytesInMessageType - bytesInUnitHeaderLength;
            //var payloadLength = shortBuffer - bytesInLong - bytesInMessageType ;
            // Console.WriteLine("shortBuffer: {0}", shortBuffer);
            // Console.WriteLine("payloadLength: {0}", payloadLength);
            
            var crc = binaryReader.ReadByte();

            var longBuffer = binaryReader.ReadBytes(bytesInLong);
            var id = BitConverter.ToInt64(longBuffer, 0);

            lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            // Console.WriteLine("lengthBuffer(raw): {0}",lengthBuffer);
            var stringLength = BitConverter.ToUInt16(lengthBuffer, 0); //Pascal string

            // Console.WriteLine("bytesInMessageType: {0}",bytesInMessageType);
            // Console.WriteLine("stringLength: {0}",stringLength);

            var messageBuffer = binaryReader.ReadBytes(bytesInMessageType - stringLength);
            var messageType = Encoding.ASCII.GetString(messageBuffer);

            lengthBuffer = binaryReader.ReadBytes(bytesInUnitHeaderLength);
            var unitHeaderLength = BitConverter.ToUInt32(lengthBuffer, 0); //ignored

            var data = binaryReader.ReadBytes(payloadLength);

            return new ExtractionMessage(){
                Length = (ushort) unitHeaderLength,
                Crc = crc,
                Id = id,
                MessageType = messageType,
                Data = data,
                MessageTypeString = "Header"
            };
        }
    }
    public struct TimeMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Second; // 4 bytes
        public long HeaderId; // 8 bytes
        public string MessageTypeString;
        public static TimeMessage BuildTimeMessage(BinaryReader binaryReader, long headerId){
            const int bytesInUInt = 4;
            const int bytesInShort = 2;


            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            // var messageType = messageTypeBuffer;

            var secondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var second = BitConverter.ToUInt32(secondBuffer, 0);

            return new TimeMessage(){
                Length = length,
                MessageType = messageType,
                Second = second,
                HeaderId = headerId,
                MessageTypeString = "TimeMessage"
            };
        }
    }
    public struct SystemEvent{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public byte EventCode; // 1 byte
        public long HeaderId; // 8 bytes
        public string MessageTypeString;
        public static SystemEvent BuildSystemEventMessage(BinaryReader binaryReader, long headerId){
            const int bytesInUInt = 4;
            const int bytesInShort = 2;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var eventCode = binaryReader.ReadByte();

            return new SystemEvent(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                EventCode = eventCode,
                HeaderId = headerId,
                MessageTypeString = "SystemEvent"
            };
        }
    }
    public struct AddOrderMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public ulong OrderID; // 8 bytes
        public byte Side; // 1 byte
        public uint Quantity; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public byte Reserved1; // 1 byte
        public byte Reserved2; // 1 byte

        public long Price; // 8 bytes
        public byte Flags; // 1 byte
        public long HeaderId; // 8 bytes
        public string MessageTypeString;

        public static AddOrderMessage BuildAddOrderMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer, 0);

            var side = binaryReader.ReadByte();

            var quantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var quantity = BitConverter.ToUInt32(quantityBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);

            var reserved1 = binaryReader.ReadByte();
            var reserved2 = binaryReader.ReadByte();

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer, 0);

            var flags = binaryReader.ReadByte();

            return new AddOrderMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                Side = side,
                Quantity  = quantity,
                InstrumentID = instrumentID,
                Reserved1 = reserved1,
                Reserved2 = reserved2,
                Price = price,
                Flags = flags,
                HeaderId = headerId,
                MessageTypeString = "AddOrder"

            };
        }
    }

    public struct AddAttributedOrderMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public ulong OrderID; // 8 bytes
        public byte Side; // 1 byte
        public uint Quantity; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public long Price; // 8 bytes
        public string Attribution; // 11 bytes, alpha/ascii
        public byte Flags; // 1 byte
        public long HeaderId; // 8 bytes
        public string MessageTypeString;

        public static AddAttributedOrderMessage BuildAddAttributedOrderMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer, 0);

            var side = binaryReader.ReadByte();

            var quantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var quantity = BitConverter.ToUInt32(quantityBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer, 0);

            var attributionBuffer = binaryReader.ReadBytes(11);
            var attribution = Encoding.ASCII.GetString(attributionBuffer);

            var flags = binaryReader.ReadByte();

            return new AddAttributedOrderMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                Side = side,
                Quantity  = quantity,
                InstrumentID = instrumentID,
                Price = price,
                Attribution = attribution,
                Flags = flags,
                HeaderId = headerId,
                MessageTypeString = "AddAttributedOrder"

            };
        }
    }
    public struct SymbolDirectoryMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public byte Reserved1; // 1 byte
        public byte Reserved2; // 1 byte
        public string SymbolStatus; // 1 byte, alpha/ascii
        public string ISIN; // 12 bytes, alpha/ascii
        public string Symbol; // 25 bytes, alpha/ascii
        public string TIDM; // 12 bytes, alpha/ascii
        public string Segment; // 6 bytes, alpha/ascii
        public long PrevClosePrice; // 8 bytes
        public string ExpirationDate; // 8 bytes, ascii YYYYMMDD
        public string Underlying; // 25 bytes, alpha/ascii
        public long StrikePrice; // 8 bytes
        public string OptionType; // 1 byte, alpha/ascii
        public string Issuer; // 6 bytes, alpha/ascii
        public string IssueDate; // 8 bytes, ascii YYYYMMDD
        public long Coupon; // 8 bytes
        public byte Flags; // 1 byte
        public byte SubBook; // 1 byte
        public string CorporateAction; // 189 bytes, alpha/ascii
        public long HeaderId; // 8 bytes
        public string MessageTypeString;

        public static SymbolDirectoryMessage BuildSymbolDirectoryMessage (BinaryReader binaryReader, long headerId){
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;
            const int bytesInDate = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer);

            var reserved1 = binaryReader.ReadByte();

            var reserved2 = binaryReader.ReadByte();

            var symbolStatusBufferr = binaryReader.ReadBytes(1);
            var symbolStatus = Encoding.ASCII.GetString(symbolStatusBufferr);

            var isinBuffer = binaryReader.ReadBytes(12);
            var isin = Encoding.ASCII.GetString(isinBuffer);

            var symbolBuffer = binaryReader.ReadBytes(25);
            var symbol = Encoding.ASCII.GetString(symbolBuffer);

            var tidmBuffer = binaryReader.ReadBytes(12);
            var tidm = Encoding.ASCII.GetString(tidmBuffer);

            var segmentBuffer = binaryReader.ReadBytes(6);
            var segment = Encoding.ASCII.GetString(segmentBuffer);

            var prevClosePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var prevClosePrice = BitConverter.ToInt64(prevClosePriceBuffer, 0);

            var expirationDateBuffer = binaryReader.ReadBytes(bytesInDate);
            var expirationDate = Encoding.ASCII.GetString(expirationDateBuffer);

            var underlyingBuffer = binaryReader.ReadBytes(25);
            var underlying = Encoding.ASCII.GetString(underlyingBuffer);

            var strikePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var strikePrice = BitConverter.ToInt64(strikePriceBuffer, 0);

            var optionTypeBuffer = binaryReader.ReadBytes(1);
            var optionType = Encoding.ASCII.GetString(optionTypeBuffer);

            var issuerBuffer = binaryReader.ReadBytes(6);
            var issuer = Encoding.ASCII.GetString(issuerBuffer);

            var issueDateBuffer = binaryReader.ReadBytes(bytesInDate);
            var issueDate = Encoding.ASCII.GetString(issueDateBuffer);

            var couponBuffer = binaryReader.ReadBytes(bytesInPrice);
            var coupon = BitConverter.ToInt64(couponBuffer, 0);

            var flags = binaryReader.ReadByte();

            var subBook = binaryReader.ReadByte();

            var corporateActionBuffer = binaryReader.ReadBytes(189);
            var corporateAction = Encoding.ASCII.GetString(corporateActionBuffer);

            return new SymbolDirectoryMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                InstrumentID = instrumentID,
                Reserved1 = reserved1,
                Reserved2 = reserved2,
                SymbolStatus = symbolStatus,
                ISIN = isin,
                Symbol = symbol,
                TIDM = tidm,
                Segment = segment,
                PrevClosePrice = prevClosePrice,
                ExpirationDate = expirationDate,
                Underlying = underlying,
                StrikePrice = strikePrice,
                OptionType = optionType,
                Issuer = issuer,
                IssueDate = issueDate,
                Coupon = coupon,
                Flags = flags,
                SubBook = subBook,
                CorporateAction = corporateAction,
                HeaderId = headerId,
                MessageTypeString = "SymbolDirectory"
            };

        }
    }
    public struct OrderDeletedMessage{
        public ushort Length;
        public string MessageType;
        public uint Nanosecond;
        public ulong OrderID;
        public long HeaderId;
        public string MessageTypeString;
        
        public static OrderDeletedMessage BuildOrderDeletedMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            
            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer, 0);

            return new OrderDeletedMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                HeaderId = headerId,
                MessageTypeString = "OrderDeleted"
            };
        }
    }
    public struct OrderModifiedMessage{
        public ushort Length;
        public string MessageType;
        public uint Nanosecond;
        public ulong OrderID;
        public uint NewQuantity;
        public long NewPrice;
        public byte Flags;
        public long HeaderId;
        public string MessageTypeString;

        public static OrderModifiedMessage BuildOrderModifiedMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;
            
            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes 

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer, 0);

            var newQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var newQuantity = BitConverter.ToUInt32(newQuantityBuffer, 0);

            var newPriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var newPrice = BitConverter.ToInt64(newPriceBuffer, 0);

            var flags = binaryReader.ReadByte();

            return new OrderModifiedMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                NewQuantity = newQuantity,
                NewPrice = newPrice,
                Flags = flags,
                HeaderId = headerId,
                MessageTypeString = "OrderModified"
            };
        }
    }
    public struct OrderBookClearMessage{
        public ushort Length;
        public string MessageType;
        public uint Nanosecond;
        public uint InstrumentID;
        public byte SubBook;
        public byte BookType;
        public long HeaderId;
        public string MessageTypeString;

        public static OrderBookClearMessage BuildOrderBookClearMessage(BinaryReader binaryReader, long headerId){
            const int bytesInUInt = 4;
            const int bytesInShort = 2;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);

            var subBook = binaryReader.ReadByte();
            var bookType = binaryReader.ReadByte();

            return new OrderBookClearMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                InstrumentID = instrumentID,
                SubBook = subBook,
                BookType = bookType,
                HeaderId = headerId,
                MessageTypeString = "OrderBookClear"
            };
        }
    }
    public struct OrderExecutedMessage{
        public ushort Length;
        public string MessageType;
        public uint Nanosecond;
        public ulong OrderID;
        public uint ExecutedQuantity;
        public ulong TradeID;
        public long LastOptPx;
        public long Volatility;
        public long UnderlyingReferencePrice;
        public long HeaderId;
        public string MessageTypeString;

        public static OrderExecutedMessage BuildOrderExecutedMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer);

            var executedQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var executedQuantity = BitConverter.ToUInt32(executedQuantityBuffer, 0);

            var tradeIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var tradeID = BitConverter.ToUInt64(tradeIDBuffer);

            var lastOptPxBuffer = binaryReader.ReadBytes(bytesInPrice);
            var lastOptPx = BitConverter.ToInt64(lastOptPxBuffer);

            var volatilityBuffer = binaryReader.ReadBytes(bytesInPrice);
            var volatility = BitConverter.ToInt64(volatilityBuffer);

            var underlyingReferencePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var underlyingReferencePrice = BitConverter.ToInt64(underlyingReferencePriceBuffer);

            return new OrderExecutedMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                ExecutedQuantity = executedQuantity,
                TradeID = tradeID,
                LastOptPx = lastOptPx,
                Volatility = volatility,
                UnderlyingReferencePrice = underlyingReferencePrice,
                HeaderId = headerId,
                MessageTypeString = "OrderExecuted"
            };
        }
    }
    public struct OrderExecutedWithPriceSizeMessage{
        public ushort Length;
        public string MessageType;
        public uint Nanosecond;
        public ulong OrderID;
        public uint ExecutedQuantity;
        public uint DisplayQuantity;
        public ulong TradeID;
        public byte Printable;
        public long Price;
        public long LastOptPx;
        public long Volatility;
        public long UnderlyingReferencePrice;
        public long HeaderId;
        public string MessageTypeString;

        public static OrderExecutedWithPriceSizeMessage BuildOrderExecutedWithPriceSizeMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var orderIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var orderID = BitConverter.ToUInt64(orderIDBuffer);

            var executedQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var executedQuantity = BitConverter.ToUInt32(executedQuantityBuffer, 0);

            var displayQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var displayQuantity = BitConverter.ToUInt32(displayQuantityBuffer, 0);

            var tradeIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var tradeID = BitConverter.ToUInt64(tradeIDBuffer);

            var printable = binaryReader.ReadByte();

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer, 0);

            var lastOptPxBuffer = binaryReader.ReadBytes(bytesInPrice);
            var lastOptPx = BitConverter.ToInt64(lastOptPxBuffer);

            var volatilityBuffer = binaryReader.ReadBytes(bytesInPrice);
            var volatility = BitConverter.ToInt64(volatilityBuffer);

            var underlyingReferencePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var underlyingReferencePrice = BitConverter.ToInt64(underlyingReferencePriceBuffer);

            return new OrderExecutedWithPriceSizeMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                OrderID = orderID,
                ExecutedQuantity = executedQuantity,
                DisplayQuantity = displayQuantity,
                TradeID = tradeID,
                Printable = printable,
                Price = price,
                LastOptPx = lastOptPx,
                Volatility = volatility,
                UnderlyingReferencePrice = underlyingReferencePrice,
                HeaderId = headerId,
                MessageTypeString = "OrderExecutedWithPriceSize"
            };
        }
    }
    public struct TradeMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public uint ExecutedQuantity; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public byte Reserved1; // 1 byte
        public byte Reserved2; // 1 byte
        public long Price; // 8 bytes
        public ulong TradeID; // 8 bytes
        public byte SubBook; //  1 byte
        public byte Flags; // 1 byte
        public string TradeSubType; //4 bytes, alpha/ascii
        public long LastOptPx; //8 bytes
        public long Volatility; // 8 bytes
        public long UnderlyingReferencePrice; // 8 bytes
        public long HeaderId; // 8 bytes
        public string MessageTypeString;
        
        public static TradeMessage BuildTradeMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var executedQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var executedQuantity = BitConverter.ToUInt32(executedQuantityBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);
            
            var reserved1 = binaryReader.ReadByte();
            var reserved2 = binaryReader.ReadByte();

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer);

            var tradeIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var tradeID = BitConverter.ToUInt64(tradeIDBuffer);

            var subBook = binaryReader.ReadByte();
            
            var flags = binaryReader.ReadByte();

            var tradeSubTypeBuffer = binaryReader.ReadBytes(4);
            var tradeSubType = Encoding.ASCII.GetString(tradeSubTypeBuffer);

            var lastOptPxBuffer = binaryReader.ReadBytes(bytesInPrice);
            var lastOptPx = BitConverter.ToInt64(lastOptPxBuffer);

            var volatilityBuffer = binaryReader.ReadBytes(bytesInPrice);
            var volatility = BitConverter.ToInt64(volatilityBuffer);

            var underlyingReferencePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var underlyingReferencePrice = BitConverter.ToInt64(underlyingReferencePriceBuffer);

            return new TradeMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                ExecutedQuantity = executedQuantity,
                InstrumentID = instrumentID,
                Reserved1 = reserved1,
                Reserved2 = reserved2,
                Price = price,
                TradeID = tradeID,
                SubBook = subBook,
                Flags = flags,
                TradeSubType = tradeSubType,
                LastOptPx = lastOptPx,
                Volatility = volatility,
                UnderlyingReferencePrice = underlyingReferencePrice,
                HeaderId = headerId,
                MessageTypeString = "Trade"
            };
        }
    }
    public struct AuctionTradeMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public uint ExecutedQuantity; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public byte Reserved1; // 1 byte
        public byte Reserved2; // 1 byte
        public long Price; // 8 bytes
        public ulong TradeID; // 8 bytes
        public byte AuctionType; //  1 byte
        public long LastOptPx; //8 bytes
        public long Volatility; // 8 bytes
        public long UnderlyingReferencePrice; // 8 bytes
        public long HeaderId; // 8 bytes
        public string MessageTypeString;
        
        public static AuctionTradeMessage BuildAuctionTradeMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var executedQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var executedQuantity = BitConverter.ToUInt32(executedQuantityBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);
            
            var reserved1 = binaryReader.ReadByte();
            var reserved2 = binaryReader.ReadByte();

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer);

            var tradeIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var tradeID = BitConverter.ToUInt64(tradeIDBuffer);

            var auctionType = binaryReader.ReadByte();

            var lastOptPxBuffer = binaryReader.ReadBytes(bytesInPrice);
            var lastOptPx = BitConverter.ToInt64(lastOptPxBuffer);

            var volatilityBuffer = binaryReader.ReadBytes(bytesInPrice);
            var volatility = BitConverter.ToInt64(volatilityBuffer);

            var underlyingReferencePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var underlyingReferencePrice = BitConverter.ToInt64(underlyingReferencePriceBuffer);

            return new AuctionTradeMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                ExecutedQuantity = executedQuantity,
                InstrumentID = instrumentID,
                Reserved1 = reserved1,
                Reserved2 = reserved2,
                Price = price,
                TradeID = tradeID,
                AuctionType = auctionType,
                LastOptPx = lastOptPx,
                Volatility = volatility,
                UnderlyingReferencePrice = underlyingReferencePrice,
                HeaderId = headerId,
                MessageTypeString = "AuctionTrade"
            };
        }
    }
    public struct OffBookTradeMessage{
        public ushort Length; // 2 bytes
        public string MessageType; // 1 byte
        public uint Nanosecond; // 4 bytes
        public uint ExecutedQuantity; // 4 bytes
        public uint InstrumentID; // 4 bytes
        public byte Reserved1; // 1 byte
        public byte Reserved2; // 1 byte
        public long Price; // 8 bytes
        public ulong TradeID; // 8 bytes
        public string OffBookTradeType; // 4 bytes, alpha/ascii
        public string TradeTime; // 8 bytes, alpha/ascii HH:MM:SS
        public string TradeDate; // 8 bytes, alpha/ascii YYYYMMDD
        public long LastOptPx; //8 bytes
        public long Volatility; // 8 bytes
        public long UnderlyingReferencePrice; // 8 bytes
        public long HeaderId; // 8 bytes
        public string MessageTypeString;
        
        public static OffBookTradeMessage BuildOffBookTradeMessage(BinaryReader binaryReader, long headerId){
            const int bytesInLong = 8;
            const int bytesInUInt = 4;
            const int bytesInShort = 2;
            const int bytesInPrice = 8;
            const int bytesInTime = 8;
            const int bytesInDate = 8;

            var UnitHeader = binaryReader.ReadBytes(8); // 8 bytes

            var lengthBuffer = binaryReader.ReadBytes(bytesInShort);
            var length = BitConverter.ToUInt16(lengthBuffer, 0);

            var messageTypeBuffer = binaryReader.ReadBytes(1);
            var messageType = Encoding.ASCII.GetString(messageTypeBuffer);
            //var messageType = messageTypeBuffer;

            var nanoSecondBuffer = binaryReader.ReadBytes(bytesInUInt);
            var nanoSecond = BitConverter.ToUInt32(nanoSecondBuffer, 0);

            var executedQuantityBuffer = binaryReader.ReadBytes(bytesInUInt);
            var executedQuantity = BitConverter.ToUInt32(executedQuantityBuffer, 0);

            var instrumentIDBuffer = binaryReader.ReadBytes(bytesInUInt);
            var instrumentID = BitConverter.ToUInt32(instrumentIDBuffer, 0);
            
            var reserved1 = binaryReader.ReadByte();
            var reserved2 = binaryReader.ReadByte();

            var priceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var price = BitConverter.ToInt64(priceBuffer);

            var tradeIDBuffer = binaryReader.ReadBytes(bytesInLong);
            var tradeID = BitConverter.ToUInt64(tradeIDBuffer);

            var offBookTradeTypeBuffer = binaryReader.ReadBytes(4);
            var offBookTradeType = Encoding.ASCII.GetString(offBookTradeTypeBuffer);

            var tradeTimeBuffer = binaryReader.ReadBytes(bytesInTime);
            var tradeTime = Encoding.ASCII.GetString(tradeTimeBuffer);

            var tradeDateBuffer = binaryReader.ReadBytes(bytesInDate);
            var tradeDate = Encoding.ASCII.GetString(tradeDateBuffer);

            var lastOptPxBuffer = binaryReader.ReadBytes(bytesInPrice);
            var lastOptPx = BitConverter.ToInt64(lastOptPxBuffer);

            var volatilityBuffer = binaryReader.ReadBytes(bytesInPrice);
            var volatility = BitConverter.ToInt64(volatilityBuffer);

            var underlyingReferencePriceBuffer = binaryReader.ReadBytes(bytesInPrice);
            var underlyingReferencePrice = BitConverter.ToInt64(underlyingReferencePriceBuffer);

            return new OffBookTradeMessage(){
                Length = length,
                MessageType = messageType,
                Nanosecond = nanoSecond,
                ExecutedQuantity = executedQuantity,
                InstrumentID = instrumentID,
                Reserved1 = reserved1,
                Reserved2 = reserved2,
                Price = price,
                TradeID = tradeID,
                OffBookTradeType = offBookTradeType,
                TradeTime = tradeTime,
                TradeDate = tradeDate,
                LastOptPx = lastOptPx,
                Volatility = volatility,
                UnderlyingReferencePrice = underlyingReferencePrice,
                HeaderId = headerId,
                MessageTypeString = "OffBookTrade"
            };
        }
    }
    static void Main(string[] args){
            
            string path = "/Users/zeroklone/projects/jse_datamine/JSE_EQM_MITCHDATA_20190805.bin";
            FileInfo fi1 = new FileInfo(path);
            StartCaptureSample(fi1, "JSE_EQM_MITCHDATA_20190805.json");
            Console.WriteLine("JSE_EQM_MITCHDATA_20190805");

            // Console.WriteLine( 
            // "This example of the BitConverter.IsLittleEndian field " +
            // "generates \nthe following output when run on " +
            // "x86-class computers.\n");
            // Console.WriteLine( "IsLittleEndian:  {0}", 
            // BitConverter.IsLittleEndian );
        }
}