using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TradeServer
{
    class Program
    {
        static bool Debug = true;

        static void Main(string[] args)
        {
            Task.Run(() => MainLoop());
            if (Debug)
                Task.Run(() => DebugLoop());
            Thread.Sleep(Timeout.Infinite);
        }

        static ConcurrentQueue<In> InQueue = new ConcurrentQueue<In>();
        static ConcurrentQueue<Out> OutQueue = new ConcurrentQueue<Out>();
        static LinkedList<OrderList> SellBook = new LinkedList<OrderList>();
        static LinkedList<OrderList> BuyBook = new LinkedList<OrderList>();

        static void DebugLoop()
        {
            while (true)
            {
                string inp = Console.ReadLine();
                if (inp == "file")
                {
                    var sr = new StreamReader("save.txt");
                    while (!sr.EndOfStream)
                    {
                        DebugFunc(sr.ReadLine(), false);
                    }
                    sr.Close();
                }
                else
                {
                    DebugFunc(inp, true);
                }
            }
        }

        static void DebugFunc(string inp, bool save)
        {
            string[] spl = inp.Split(' ');
            var tt = TradeType.BUY;
            bool error = false;
            switch (spl[0].ToUpper())
            {
                case "BUY":
                    tt = TradeType.BUY;
                    break;
                case "SELL":
                    tt = TradeType.SELL;
                    break;
                default:
                    Console.WriteLine("error");
                    error = true;
                    break;
            }
            if (false == error)
            {
                int price = int.Parse(spl[1]);
                int amount = int.Parse(spl[2]);

                if (save)
                {
                    var sw = new StreamWriter("save.txt", true);
                    sw.WriteLine(inp);
                    sw.Close();
                }

                InQueue.Enqueue(new In()
                {
                    Type = tt,
                    UserId = 777,
                    Price = price,
                    Amount = amount
                });
            }
        }

        static void MainLoop()
        {
            while (true)
            {
                In inData;
                if (InQueue.TryDequeue(out inData))
                {
                    switch (inData.Type)
                    {
                        case TradeType.BUY:
                            {
                                if (null == SellBook.First || inData.Price < SellBook.First.Value.Price)
                                {
                                    // 가장 낮은 판매가격보다 더 낮은 가격으로 매수신청하였으므로 매수오더북에 추가
                                    RegistBuyBook(inData.UserId, inData.Price, inData.Amount);
                                }
                                else
                                {
                                    // 가장 낮은 판매가격 이상으로 매수신청하였으므로 즉시체결
                                    int amount = inData.Amount;
                                    var ptr = SellBook.First;
                                    while (0 < amount && null != ptr)
                                    {
                                        if (inData.Price < ptr.Value.Price)
                                            break;

                                        var applyResult = ptr.Value.ApplyMatch(amount);
                                        if (applyResult.Cleared)
                                            SellBook.RemoveFirst();
                                        amount -= applyResult.MatchAmount;
                                        foreach (var match in applyResult.MatchList)
                                        {
                                            OutQueue.Enqueue(new Out()
                                            {
                                                Type = TradeType.SELL,
                                                UserId = match.UserId,
                                                Amount = match.Amount,
                                                Price = ptr.Value.Price
                                            });
                                        }
                                        OutQueue.Enqueue(new Out()
                                        {
                                            Type = TradeType.BUY,
                                            UserId = inData.UserId,
                                            Amount = applyResult.MatchAmount,
                                            Price = ptr.Value.Price
                                        });

                                        ptr = ptr.Next;
                                    }
                                    if (0 < amount)
                                    {
                                        RegistBuyBook(inData.UserId, inData.Price, amount);
                                    }
                                }
                            }
                            break;
                        case TradeType.SELL:
                            {
                                if (null == BuyBook.First || BuyBook.First.Value.Price < inData.Price)
                                {
                                    // 가장 높은 구매가격보다 더 높은 가격으로 매도신청하였으므로 매도오더북에 추가
                                    RegistSellBook(inData.UserId, inData.Price, inData.Amount);
                                }
                                else
                                {
                                    // 가장 높은 구매가격 이하로 매도신청하였으므로 즉시체결
                                    int amount = inData.Amount;
                                    var ptr = BuyBook.First;
                                    while (0 < amount && null != ptr)
                                    {
                                        if (ptr.Value.Price < inData.Price)
                                            break;

                                        var applyResult = ptr.Value.ApplyMatch(amount);
                                        if (applyResult.Cleared)
                                            BuyBook.RemoveFirst();
                                        amount -= applyResult.MatchAmount;
                                        foreach (var match in applyResult.MatchList)
                                        {
                                            OutQueue.Enqueue(new Out()
                                            {
                                                Type = TradeType.BUY,
                                                UserId = match.UserId,
                                                Amount = match.Amount,
                                                Price = ptr.Value.Price
                                            });
                                        }
                                        OutQueue.Enqueue(new Out()
                                        {
                                            Type = TradeType.SELL,
                                            UserId = inData.UserId,
                                            Amount = applyResult.MatchAmount,
                                            Price = ptr.Value.Price
                                        });

                                        ptr = ptr.Next;
                                    }
                                    if (0 < amount)
                                    {
                                        RegistSellBook(inData.UserId, inData.Price, amount);
                                    }
                                }
                            }
                            break;
                    }

                    if (Debug)
                    {
                        var scr = new string[14];
                        int idx = scr.Length / 2 - 1;
                        foreach (var sell in SellBook)
                        {
                            scr[idx] = string.Format($"{sell.GetAmount(), 10} {sell.Price, 8}");
                            if (0 == idx)
                                break;
                            idx--;
                        }
                        idx = scr.Length / 2;
                        foreach (var buy in BuyBook)
                        {
                            scr[idx] = string.Format($"{" ", 10} {buy.Price, 8}    {buy.GetAmount(), -10}");
                            if (scr.Length - 1 == idx)
                                break;
                            idx++;
                        }
                        Console.Clear();
                        foreach (string str in scr)
                            Console.WriteLine(str);
                    }
                }
                Thread.Sleep(1);
            }
        }

        static void RegistBuyBook(int userId, int price, int amount)
        {
            var ptr = BuyBook.First;
            while (ptr != null)
            {
                if (price == ptr.Value.Price)
                {
                    ptr.Value.AddOrder(userId, amount);
                    return;
                }

                if (ptr.Value.Price < price)
                {
                    var ol = new OrderList();
                    ol.Price = price;
                    ol.AddOrder(userId, amount);
                    BuyBook.AddBefore(ptr, ol);
                    return;
                }

                ptr = ptr.Next;
            }
            {
                var ol = new OrderList();
                ol.Price = price;
                ol.AddOrder(userId, amount);
                BuyBook.AddLast(ol);
            }
        }

        static void RegistSellBook(int userId, int price, int amount)
        {
            var ptr = SellBook.First;
            while (ptr != null)
            {
                if (price == ptr.Value.Price)
                {
                    ptr.Value.AddOrder(userId, amount);
                    return;
                }

                if (price < ptr.Value.Price)
                {
                    var ol = new OrderList();
                    ol.Price = price;
                    ol.AddOrder(userId, amount);
                    SellBook.AddBefore(ptr, ol);
                    return;
                }

                ptr = ptr.Next;
            }
            {
                var ol = new OrderList();
                ol.Price = price;
                ol.AddOrder(userId, amount);
                SellBook.AddLast(ol);
            }
        }
    }

    public class Order
    {
        public int UserId;
        public int Amount;
    }

    public class OrderList
    {
        public int Price;
        private LinkedList<Order> List = new LinkedList<Order>();

        public void AddOrder(int userId, int amount)
        {
            List.AddLast(new Order()
            {
                UserId = userId,
                Amount = amount
            });
        }

        public int GetAmount()
        {
            return List.Sum(d => d.Amount);
        }

        public ApplyMatchOutput ApplyMatch(int amount)
        {
            var amo = new ApplyMatchOutput();
            int removeCount = 0;
            foreach (var dat in List)
            {
                if (amount < dat.Amount)
                {
                    dat.Amount -= amount;
                    amo.Add(dat.UserId, amount);
                    break;
                }
                else if (dat.Amount < amount)
                {
                    removeCount++;
                    amo.Add(dat.UserId, dat.Amount);
                    amount -= dat.Amount;
                }
                else
                {
                    removeCount++;
                    amo.Add(dat.UserId, amount);
                    break;
                }
            }
            for (int i = 0; i < removeCount; i++)
            {
                List.RemoveFirst();
            }
            amo.Cleared = (0 == List.Count);
            return amo;
        }

        public class ApplyMatchOutput
        {
            public int MatchAmount = 0;
            public List<Order> MatchList = new List<Order>();
            public bool Cleared = false;

            public void Add(int userId, int amount)
            {
                MatchList.Add(new Order()
                {
                    UserId = userId,
                    Amount = amount
                });
                MatchAmount += amount;
            }
        }
    }

    public enum TradeType
    {
        BUY,
        SELL,
    }

    public class In
    {
        public TradeType Type;
        public int UserId;      // 주문자ID
        public int Price;       // 가격
        public int Amount;      // 수량
        public bool Cancel;     // true : 취소주문, false : 정상주문
    }

    public class Out
    {
        public TradeType Type;
        public int UserId;
        public int Price;
        public int Amount;
    }
}