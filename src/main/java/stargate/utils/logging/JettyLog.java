/*
   Copyright 2018 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package stargate.utils.logging;

import org.eclipse.jetty.util.log.AbstractLogger;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 *
 * @author iychoi
 */
public class JettyLog extends AbstractLogger {
    private org.apache.commons.logging.Log _logger;
    private String _name;
    private boolean _debugEnalbed;
    
    public JettyLog() {
        this(JettyLog.class.getName());
    }

    public JettyLog(String name) {
        this._logger = org.apache.commons.logging.LogFactory.getLog(name);
        this._name = name;
    }

    @Override
    public String getName() {
        return this._name;
    }

    @Override
    public void warn(String msg, Object... args) {
        this._logger.warn(format(msg, args));
    }

    @Override
    public void warn(Throwable thrown) {
        this._logger.warn(thrown);
    }

    @Override
    public void warn(String msg, Throwable thrown) {
        this._logger.warn(msg, thrown);
    }

    @Override
    public void info(String msg, Object... args) {
        this._logger.info(format(msg, args));
    }

    @Override
    public void info(Throwable thrown) {
        this._logger.info(thrown);
    }

    @Override
    public void info(String msg, Throwable thrown) {
        this._logger.info(msg, thrown);
    }

    @Override
    public boolean isDebugEnabled() {
        return this._debugEnalbed;
    }

    @Override
    public void setDebugEnabled(boolean enabled) {
        this._debugEnalbed = enabled;
    }

    @Override
    public void debug(String msg, Object... args) {
        if(this._debugEnalbed) {
            this._logger.debug(format(msg, args));
        }
    }

    @Override
    public void debug(String msg, long arg) {
        if(this._debugEnalbed) {
            this._logger.debug(format(msg, arg));
        }
    }

    @Override
    public void debug(Throwable thrown) {
        if(this._debugEnalbed) {
            this._logger.debug(thrown);
        }
    }

    @Override
    public void debug(String msg, Throwable thrown) {
        if(this._debugEnalbed) {
            this._logger.debug(msg, thrown);
        }
    }

    @Override
    protected Logger newLogger(String fullname) {
        return new JettyLog(fullname);
    }

    @Override
    public void ignore(Throwable ignored) {
        this._logger.warn(Log.IGNORED, ignored);
    }

    private String format(String msg, Object... args) {
        msg = String.valueOf(msg);
        String braces = "{}";
        StringBuilder builder = new StringBuilder();
        int start = 0;
        for (Object arg : args) {
            int bracesIndex = msg.indexOf(braces, start);
            if (bracesIndex < 0) {
                builder.append(msg.substring(start));
                builder.append(" ");
                builder.append(arg);
                start = msg.length();
            } else {
                builder.append(msg.substring(start, bracesIndex));
                builder.append(String.valueOf(arg));
                start = bracesIndex + braces.length();
            }
        }
        builder.append(msg.substring(start));
        return builder.toString();
    }
}
