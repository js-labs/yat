/*
 * Copyright (C) 2022 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of YAT.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.yat.server;

import org.jsl.yat.Protocol;
import java.io.File;

public class Main
{
    public static void main(String[] args)
    {
        if (args.length > 0)
        {
            String storagePath = null;
            int portNumber = Protocol.SERVER_PORT;

            for (int idx=0; idx<args.length; idx++)
            {
                if (args[idx].equals("-s"))
                {
                    if (++idx == args.length)
                    {
                        System.out.println("Missing storage path");
                        System.exit(-1);
                        return;
                    }
                    storagePath = args[idx];
                    final File f = new File(storagePath);
                    if (!f.exists())
                    {
                        System.out.println("'" + storagePath + "' does not exist");
                        System.exit(-1);
                        return;
                    }
                    if (!f.isDirectory())
                    {
                        System.out.println("'" + storagePath + "' is not a directory");
                        System.exit(-1);
                        return;
                    }
                }
                else if (args[idx].equals("-p"))
                {
                    if (++idx == args.length)
                    {
                        System.out.println("Missing port number");
                        System.exit(-1);
                        return;
                    }

                    try
                    {
                        portNumber = Integer.parseInt(args[idx]);
                    }
                    catch (final NumberFormatException ex)
                    {
                        System.out.println("Invalid port number '" + args[idx] + "'");
                        System.exit(-1);
                        return;
                    }
                }
                else
                {
                    System.out.println("Unexpected command line option '" + args[idx] + "'");
                    System.exit(-1);
                    return;
                }
            }

            if (storagePath == null)
                System.out.println("Missing storage path");
            else
            {
                final Server server = new Server(storagePath);
                server.run(portNumber);
                return;
            }
        }

        System.out.println("Usage: YAT-server -s <storage directory> [-p <port number>]");
    }
}
